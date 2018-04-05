/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.raft.controller;

import io.zeebe.raft.Loggers;
import io.zeebe.raft.Raft;
import io.zeebe.raft.protocol.JoinRequest;
import io.zeebe.raft.protocol.JoinResponse;
import io.zeebe.raft.protocol.LeaveRequest;
import io.zeebe.raft.protocol.LeaveResponse;
import io.zeebe.transport.*;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

import java.time.Duration;

public class MembershipController
{
    private static final Logger LOG = Loggers.RAFT_LOGGER;

    public static final Duration DEFAULT_JOIN_TIMEOUT = Duration.ofMillis(500);
    public static final Duration DEFAULT_JOIN_RETRY = Duration.ofMillis(200);

    public static final Duration DEFAULT_LEAVE_TIMEOUT = Duration.ofMillis(500);
    public static final Duration DEFAULT_LEAVE_RETRY = Duration.ofMillis(200);

    private final ActorControl actor;
    private final JoinRequest joinRequest = new JoinRequest();
    private final JoinResponse joinResponse = new JoinResponse();

    private final LeaveRequest leaveRequest = new LeaveRequest();
    private final LeaveResponse leaveResponse = new LeaveResponse();

    private final Raft raft;

    // will not be reset to continue to select new members on every retry
    private int currentMember;
    private boolean isJoined;
    private CompletableActorFuture<Void> leaveFuture;

    public MembershipController(final Raft raft, ActorControl actorControl)
    {
        this.actor = actorControl;
        this.raft = raft;
    }

    public void join()
    {
        final RemoteAddress nextMember = getNextMember();

        if (nextMember != null)
        {
            joinRequest.reset().setRaft(raft);

            LOG.debug("Send join request to {}", nextMember);
            final ActorFuture<ClientResponse> responseFuture = raft.sendRequest(nextMember, joinRequest, DEFAULT_JOIN_TIMEOUT);

            actor.runOnCompletion(responseFuture, ((response, throwable) ->
            {
                if (throwable == null)
                {
                    try
                    {
                        final DirectBuffer responseBuffer = response.getResponseBuffer();
                        joinResponse.wrap(responseBuffer, 0, responseBuffer.capacity());
                    }
                    finally
                    {
                        response.close();
                    }

                    if (!raft.mayStepDown(joinResponse) && raft.isTermCurrent(joinResponse))
                    {
                        // update members to maybe discover leader
                        raft.addMembers(joinResponse.getMembers());

                        if (joinResponse.isSucceeded())
                        {
                            LOG.debug("Join request was accepted in term {}", joinResponse.getTerm());
                            isJoined = true;
                            // as this will not trigger a state change in raft we have to notify listeners
                            // that this raft is now in a visible state
                            raft.notifyRaftStateListeners();
                        }
                        else
                        {
                            LOG.debug("Join was not accepted!");
                            actor.runDelayed(DEFAULT_JOIN_RETRY, this::join);
                        }
                    }
                    else
                    {
                        LOG.debug("Join response with different term.");
                        // received response from different term
                        actor.runDelayed(DEFAULT_JOIN_RETRY, this::join);
                    }
                }
                else
                {
                    LOG.debug("Failed to send join request to {}", nextMember);
                    actor.runDelayed(DEFAULT_JOIN_RETRY, this::join);
                }
            }));
        }
        else
        {
            LOG.debug("Joined single node cluster.");
            isJoined = true;
        }
    }

    public void leave(CompletableActorFuture<Void> completableActorFuture)
    {
        if (isJoined)
        {
            leaveFuture = completableActorFuture;
            leave();
        }
        else
        {
            completableActorFuture.complete(null);
        }
    }

    private void leave()
    {
        final RemoteAddress nextMember = getNextMember();

        if (nextMember != null)
        {
            leaveRequest.reset().setRaft(raft);

            LOG.debug("Send leave request to {}", nextMember);
            final ActorFuture<ClientResponse> responseFuture = raft.sendRequest(nextMember, leaveRequest, DEFAULT_LEAVE_TIMEOUT);

            actor.runOnCompletion(responseFuture, ((response, throwable) ->
            {
                if (throwable == null)
                {
                    try
                    {
                        final DirectBuffer responseBuffer = response.getResponseBuffer();
                        leaveResponse.wrap(responseBuffer, 0, responseBuffer.capacity());
                    }
                    finally
                    {
                        response.close();
                    }

                    // TODO is it really necessary to be in the same term?!
                    if (!raft.mayStepDown(leaveResponse) && raft.isTermCurrent(leaveResponse))
                    {
                        if (leaveResponse.isSucceeded())
                        {
                            LOG.debug("Leave request was accepted in term {}", leaveResponse.getTerm());
                            isJoined = false;

                            // as this will not trigger a state change in raft we have to notify listeners
                            // that this raft is now in a visible state
                            raft.notifyRaftStateListeners();
                            // TODO should we close the raft ?!
//                            raft.close();
                            leaveFuture.complete(null);
                        }
                        else
                        {
                            LOG.debug("Leave was not accepted!");
                            actor.runDelayed(DEFAULT_LEAVE_RETRY, this::leave);
                        }
                    }
                    else
                    {
                        LOG.debug("Leave response with different term.");
                        // received response from different term
                        actor.runDelayed(DEFAULT_LEAVE_RETRY, this::leave);
                    }
                }
                else
                {
                    LOG.debug("Failed to send leave request to {}", nextMember);
                    actor.runDelayed(DEFAULT_LEAVE_RETRY, this::leave);
                }
            }));
        }
        else
        {
            throw new UnsupportedOperationException("Signle node can't left cluster");
//            LOG.debug("Joined single node cluster.");
//            left = true;
        }
    }

    public boolean isJoined()
    {
        return isJoined;
    }

    private RemoteAddress getNextMember()
    {
        final int memberSize = raft.getMemberSize();
        if (memberSize > 0)
        {
            final int nextMember = currentMember % memberSize;
            currentMember++;

            return raft.getMember(nextMember).getRemoteAddress();
        }
        else
        {
            return null;
        }
    }

}
