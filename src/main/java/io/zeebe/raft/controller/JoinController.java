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
import io.zeebe.transport.ClientRequest;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

import java.time.Duration;

public class JoinController
{
    private static final Logger LOG = Loggers.RAFT_LOGGER;

    public static final Duration DEFAULT_JOIN_TIMEOUT = Duration.ofMillis(500);
    public static final Duration DEFAULT_JOIN_RETRY = Duration.ofMillis(200);

    private final ActorControl actor;
    private final JoinRequest joinRequest = new JoinRequest();
    private final JoinResponse joinResponse = new JoinResponse();
    private final Raft raft;

    // will not be reset to continue to select new members on every retry
    private int currentMember;
    private boolean isJoined;

    public JoinController(final Raft raft, ActorControl actorControl)
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
            final ActorFuture<ClientRequest> requestFuture = raft.sendRequest(nextMember, joinRequest, DEFAULT_JOIN_TIMEOUT);

            actor.runOnCompletion(requestFuture, ((clientRequest, throwable) ->
            {
                if (throwable == null)
                {
                    //
                    final DirectBuffer responseBuffer = clientRequest.join();
                    joinResponse.wrap(responseBuffer, 0, responseBuffer.capacity());
                    clientRequest.close();

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
                            actor.runDelayed(DEFAULT_JOIN_RETRY, this::join);
                        }
                    }
                    else
                    {
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
            isJoined = true;
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
