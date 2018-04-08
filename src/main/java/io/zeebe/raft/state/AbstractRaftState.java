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
package io.zeebe.raft.state;

import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.*;
import io.zeebe.raft.protocol.*;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerOutput;
import io.zeebe.util.sched.*;
import io.zeebe.util.sched.channel.*;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;

public abstract class AbstractRaftState implements Service<RaftState>, MessageHandler
{
    protected final Raft raft;
    protected final ActorControl raftActor;
    protected final BufferedLogStorageAppender appender;
    protected final LogStream logStream;

    protected final ConfigurationResponse configurationResponse = new ConfigurationResponse();
    protected final PollResponse pollResponse = new PollResponse();
    protected final VoteResponse voteResponse = new VoteResponse();

    protected final AppendResponse appendResponse = new AppendResponse();

    protected final JoinRequest joinRequest = new JoinRequest();
    protected final PollRequest pollRequest = new PollRequest();
    protected final VoteRequest voteRequest = new VoteRequest();
    protected final AppendRequest appendRequest = new AppendRequest();

    protected final BufferedLogStreamReader reader;

    protected final ConcurrentQueueChannel<IncomingRaftRequest> requestQueue;
    protected ChannelSubscription requestQueueSubscription;

    protected final OneToOneRingBufferChannel messageBuffer;
    protected ChannelSubscription messageBufferSubscription;

    public AbstractRaftState(final Raft raft, ActorControl raftActor)
    {
        this.raft = raft;
        this.raftActor = raftActor;
        this.requestQueue = raft.getRequestQueue();
        this.messageBuffer = raft.getMessageReceiveBuffer();
        this.logStream = raft.getLogStream();
        this.appender = new BufferedLogStorageAppender(raft);
        this.appender.reset();

        reader = new BufferedLogStreamReader(logStream, true);
    }

    @Override
    public final void start(ServiceStartContext startContext)
    {
        startContext.async(raftActor.call(this::onEnterState));
    }

    @Override
    public final void stop(ServiceStopContext stopContext)
    {
        stopContext.async(raftActor.call(this::onLeaveState));
    }

    protected void onEnterState()
    {
        requestQueueSubscription = raftActor.consume(requestQueue, this::consumeRequest);
        messageBufferSubscription = raftActor.consume(messageBuffer, this::consumeMessage);
    }


    protected void onLeaveState()
    {
        requestQueueSubscription.cancel();
        messageBufferSubscription.cancel();
        reader.close();
    }

    private void consumeRequest()
    {
        final IncomingRaftRequest request = requestQueue.poll();

        if (request != null)
        {
            final ServerOutput output = request.getOutput();
            final RemoteAddress remoteAddress = request.getRemoteAddress();
            final long requestId = request.getRequestId();
            final MutableDirectBuffer requestData = request.getRequestData();
            final int length = requestData.capacity();

            if (joinRequest.tryWrap(requestData, 0, length))
            {
                joinRequest(output, remoteAddress, requestId, joinRequest);
            }
            else if (pollRequest.tryWrap(requestData, 0, length))
            {
                pollRequest(output, remoteAddress, requestId, pollRequest);
            }
            else if (voteRequest.tryWrap(requestData, 0, length))
            {
                voteRequest(output, remoteAddress, requestId, voteRequest);
            }
        }
    }

    protected void consumeMessage()
    {
        messageBuffer.read(this, 1);
    }

    @Override
    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
    {
        if (appendRequest.tryWrap(buffer, index, length))
        {
            appendRequest(appendRequest);
        }
        else if (appendResponse.tryWrap(buffer, index, length))
        {
            appendResponse(appendResponse);
        }
    }

    public abstract RaftState getState();

    @Override
    public RaftState get()
    {
        return getState();
    }

    public void joinRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId, final JoinRequest joinRequest)
    {
        raft.mayStepDown(configurationRequest);
        rejectConfigurationRequest(serverOutput, remoteAddress, requestId);
    }

    public void pollRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId, final PollRequest pollRequest)
    {
        raft.mayStepDown(pollRequest);

        final boolean granted = raft.isTermCurrent(pollRequest) &&
            appender.isAfterOrEqualsLastEvent(pollRequest.getLastEventPosition(), pollRequest.getLastEventTerm());

        if (granted)
        {
            Loggers.RAFT_LOGGER.debug("accept poll request");
            skipNextElection();
            acceptPollRequest(serverOutput, remoteAddress, requestId);
        }
        else
        {
            rejectPollRequest(serverOutput, remoteAddress, requestId);
        }
    }

    protected void skipNextElection()
    {
        // no-op
    }

    public void voteRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId, final VoteRequest voteRequest)
    {
        skipNextElection();
        raft.mayStepDown(voteRequest);

        final boolean granted = raft.isTermCurrent(voteRequest) &&
            raft.canVoteFor(voteRequest) &&
            appender.isAfterOrEqualsLastEvent(voteRequest.getLastEventPosition(), voteRequest.getLastEventTerm()) &&
            raft.shouldElect();

        if (granted)
        {
            if (raft.shouldElect())
            {
                raft.setVotedFor(voteRequest.getSocketAddress());
                acceptVoteRequest(serverOutput, remoteAddress, requestId);
            }
            else
            {
                Loggers.RAFT_LOGGER.debug("Reject poll, because have no heart beat timeout.");
                rejectVoteRequest(serverOutput, remoteAddress, requestId);
            }
        }
        else
        {
            rejectVoteRequest(serverOutput, remoteAddress, requestId);
        }
    }

    protected void appendRequest(final AppendRequest appendRequest)
    {
        Loggers.RAFT_LOGGER.debug("Got Append request in leader state ");
        raft.mayStepDown(appendRequest);
        rejectAppendRequest(appendRequest, appendRequest.getPreviousEventPosition());
    }

    protected void appendResponse(final AppendResponse appendResponse)
    {
        raft.mayStepDown(appendResponse);
    }

    protected void acceptConfigurationRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId)
    {
        configurationResponse
            .reset()
            .setSucceeded(true)
            .setRaft(raft);

        raft.sendResponse(serverOutput, remoteAddress, requestId, configurationResponse);
    }

    protected void rejectConfigurationRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId)
    {
        configurationResponse
            .reset()
            .setSucceeded(false)
            .setRaft(raft);

        raft.sendResponse(serverOutput, remoteAddress, requestId, configurationResponse);
    }

    protected void acceptPollRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId)
    {
        pollResponse
            .reset()
            .setTerm(raft.getTerm())
            .setGranted(true);

        raft.sendResponse(serverOutput, remoteAddress, requestId, pollResponse);
    }

    protected void rejectPollRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId)
    {
        pollResponse
            .reset()
            .setTerm(raft.getTerm())
            .setGranted(false);

        raft.sendResponse(serverOutput, remoteAddress, requestId, pollResponse);
    }

    protected void acceptVoteRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId)
    {
        voteResponse
            .reset()
            .setTerm(raft.getTerm())
            .setGranted(true);

        raft.sendResponse(serverOutput, remoteAddress, requestId, voteResponse);
    }

    protected void rejectVoteRequest(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId)
    {
        voteResponse
            .reset()
            .setTerm(raft.getTerm())
            .setGranted(false);

        raft.sendResponse(serverOutput, remoteAddress, requestId, voteResponse);
    }

    protected void rejectAppendRequest(final HasSocketAddress hasSocketAddress, final long position)
    {
        appendResponse
            .reset()
            .setRaft(raft)
            .setPreviousEventPosition(position)
            .setSucceeded(false);

        raft.sendMessage(hasSocketAddress.getSocketAddress(), appendResponse);
    }

}
