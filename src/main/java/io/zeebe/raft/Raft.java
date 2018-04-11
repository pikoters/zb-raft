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
package io.zeebe.raft;

import static io.zeebe.raft.RaftServiceNames.*;

import java.time.Duration;
import java.util.*;

import io.zeebe.logstreams.impl.service.LogStreamServiceNames;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.msgpack.value.ValueArray;
import io.zeebe.raft.controller.*;
import io.zeebe.raft.event.RaftConfigurationEventMember;
import io.zeebe.raft.protocol.*;
import io.zeebe.raft.state.*;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.*;
import io.zeebe.transport.impl.actor.Receiver;
import io.zeebe.util.LogUtil;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.channel.ConcurrentQueueChannel;
import io.zeebe.util.sched.channel.OneToOneRingBufferChannel;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

/**
 * Representation of a member of a raft cluster.
 */
public class Raft extends Actor implements ServerMessageHandler, ServerRequestHandler, Service<Raft>
{
    private static final Logger LOG = Loggers.RAFT_LOGGER;
    private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();

    private final Injector<LogStream> logStreamInjector = new Injector<>();

    // environment
    private final ConcurrentQueueChannel<IncomingRaftRequest> requestQueue = new ConcurrentQueueChannel<>(new ManyToOneConcurrentLinkedQueue<>());
    private final OneToOneRingBufferChannel messageReceiveBuffer;
    private final RaftConfiguration configuration;
    private final SocketAddress socketAddress;
    private final ClientTransport clientTransport;

    private ServiceName<?> currentStateServiceName;
    private RaftState state;

    // persistent state
    private LogStream logStream;
    private final RaftPersistentStorage persistentStorage;

    // volatile state
    private final List<RaftStateListener> raftStateListeners = new ArrayList<>();
    private final Heartbeat heartbeat;
    private final RaftMembers raftMembers;

    // reused entities
    private final TransportMessage transportMessage = new TransportMessage();
    private final ServerResponse serverResponse = new ServerResponse();

    private ServiceStartContext serviceContext;

    private String actorName;
    private String raftName;

    public Raft(final RaftConfiguration configuration,
        final SocketAddress socketAddress,
        final ClientTransport clientTransport,
        final RaftPersistentStorage persistentStorage,
        final OneToOneRingBufferChannel messageReceiveBuffer,
        final RaftStateListener... listeners)
    {
        this.configuration = configuration;
        this.socketAddress = socketAddress;
        this.clientTransport = clientTransport;
        this.persistentStorage = persistentStorage;
        this.messageReceiveBuffer = messageReceiveBuffer;
        this.actorName = String.format("%s.%s", logStream.getLogName(), socketAddress.toString());
        this.raftName = logStream.getLogName();

        this.heartbeat = new Heartbeat(configuration.getElectionIntervalMs());
        this.raftMembers = new RaftMembers(socketAddress, persistentStorage, clientTransport::registerRemoteAddress);

        raftStateListeners.addAll(Arrays.asList(listeners));

        LOG.info("Created raft with configuration: " + this.configuration);
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        this.logStream = logStreamInjector.getValue();

        this.serviceContext = startContext;

        final RaftJoinService raftJoinedService = new RaftJoinService(this, actor);
        serviceContext.createService(joinServiceName(raftName), raftJoinedService)
            .install();

        startContext.async(startContext.getScheduler().submitActor(this, true));
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(actor.close());
    }

    @Override
    protected void onActorStarted()
    {
        becomeFollower();
    }

    // state transitions

    public void becomeFollower()
    {
        final ActorFuture<Void> whenPrevStateLeft;

        if (currentStateServiceName != null)
        {
            whenPrevStateLeft = serviceContext.removeService(currentStateServiceName);
        }
        else
        {
            whenPrevStateLeft = CompletableActorFuture.completed(null);
        }

        actor.runOnCompletion(whenPrevStateLeft, (v, t) ->
        {
            final ServiceName<RaftState> followerServiceName = followerServiceName(raftName, getTerm());
            final FollowerState followerState = new FollowerState(this, actor);

            actor.runOnCompletion(serviceContext.createService(followerServiceName, followerState).install(), (v2, t2) ->
            {
                if (t2 == null)
                {
                    LOG.debug("Raft became follower in term {}", getTerm());
                    this.state = RaftState.FOLLOWER;
                    notifyRaftStateListeners();
                }
                else
                {
                    LOG.error("Could not transition to follower state ", t);
                    becomeFollower();
                }
            });

            currentStateServiceName = followerServiceName;
        });
    }

    public void becomeCandidate()
    {
        actor.runOnCompletion(serviceContext.removeService(currentStateServiceName), (v, t) ->
        {
            setTerm(getTerm() + 1);
            setVotedFor(getSocketAddress());

            final ServiceName<RaftState> candidateServiceName = candidateServiceName(raftName, getTerm());
            final CandidateState candidateState = new CandidateState(this, actor);

            final ActorFuture<Void> whenCandicate = serviceContext.createService(candidateServiceName, candidateState)
                .dependency(joinServiceName(raftName))
                .install();

            actor.runOnCompletion(whenCandicate, (v2, t2) ->
            {
                if (t2 == null)
                {
                    LOG.debug("Raft became candidate in term {}", getTerm());
                    this.state = RaftState.CANDIDATE;
                    notifyRaftStateListeners();
                }
                else
                {
                    LOG.error("Could not transition to candidate state ", t);
                    becomeFollower();
                }
            });

            currentStateServiceName = candidateServiceName;
        });
    }

    public void becomeLeader()
    {
        actor.runOnCompletion(serviceContext.removeService(currentStateServiceName), (v, t) ->
        {
            final int term = getTerm();

            final ServiceName<Void> installOperationServiceName = leaderInstallServiceName(raftName, term);
            final ServiceName<RaftState> leaderServiceName = leaderServiceName(raftName, term);
            final ServiceName<Void> initialEventCommittedServiceName = leaderInitialEventCommittedServiceName(raftName, term);
            final ServiceName<Void> openLogStreamServiceName = leaderOpenLogStreamServiceName(raftName, term);

            final CompositeServiceBuilder installOperation = serviceContext.createComposite(installOperationServiceName);

            installOperation.createService(openLogStreamServiceName, new LeaderOpenLogStreamAppenderService(logStream))
                .install();

            final LeaderState leaderState = new LeaderState(this, actor);
            installOperation.createService(leaderServiceName, leaderState)
                .dependency(LogStreamServiceNames.logWriteBufferServiceName(logStream.getLogName()))
                .dependency(openLogStreamServiceName)
                .dependency(joinServiceName(raftName))
                .install();

            final LeaderCommitInitialEvent leaderCommitInitialEventService = new LeaderCommitInitialEvent(this, actor, leaderState);
            installOperation.createService(initialEventCommittedServiceName, leaderCommitInitialEventService)
                .dependency(leaderServiceName)
                .install();

            for (RaftMember raftMember : raftMembers.getMemberList())
            {
                final ServiceName<Void> replicateLogControllerServiceName = replicateLogConrollerServiceName(raftName, term, raftMember.getRemoteAddress().getAddress());
                final MemberReplicateLogController replicationController = new MemberReplicateLogController(this, raftMember, clientTransport);
                installOperation.createService(replicateLogControllerServiceName, replicationController)
                    .dependency(leaderServiceName)
                    .install();
            }

            final ActorFuture<Void> whenLeader = installOperation.install();

            actor.runOnCompletion(whenLeader, (v2, t2) ->
            {
                if (t == null)
                {
                    LOG.debug("Raft became leader in term {}", getTerm());
                    this.state = RaftState.LEADER;
                    notifyRaftStateListeners();
                }
                else
                {
                    LOG.error("Could not transition to leader state ", t);
                    becomeFollower();
                }
            });

            currentStateServiceName = installOperationServiceName;
        });
    }

    // listeners

    public void registerRaftStateListener(final RaftStateListener listener)
    {
        actor.call(() -> raftStateListeners.add(listener));
    }

    public void removeRaftStateListener(final RaftStateListener listener)
    {
        actor.call(() -> raftStateListeners.remove(listener));
    }

    public void notifyRaftStateListeners()
    {
        raftStateListeners.forEach((l) -> LogUtil.catchAndLog(LOG, () ->
            l.onStateChange(this, state)));
    }

    private void notifyMembersChangedListeners()
    {
        final List<SocketAddress> memberAddresses = raftMembers.getMemberAddresses();

        raftStateListeners.forEach((l) -> LogUtil.catchAndLog(LOG, () ->
            l.onMembersChanged(this, memberAddresses)));
    }

    // message handler

    /**
     * called by the transport {@link Receiver}
     */
    @Override
    public boolean onMessage(final ServerOutput output, final RemoteAddress remoteAddress, final DirectBuffer buffer, final int offset, final int length)
    {
        if (IS_TRACE_ENABLED)
        {
            LOG.trace("Received message from {}", remoteAddress);
        }

        final boolean isWritten = messageReceiveBuffer.write(1, buffer, offset, length);

        if (!isWritten)
        {
            LOG.warn("dropping");
        }

        return true;
    }

    /**
     * called by the transport {@link Receiver}
     */
    @Override
    public boolean onRequest(final ServerOutput output, final RemoteAddress remoteAddress, final DirectBuffer buffer, final int offset, final int length, final long requestId)
    {
        final MutableDirectBuffer requestData = new UnsafeBuffer(new byte[length]);
        requestData.putBytes(0, buffer, offset, length);

        return requestQueue.offer(new IncomingRaftRequest(output,
            remoteAddress,
            requestId,
            requestData));
    }


    // environment

    public RaftConfiguration getConfiguration()
    {
        return configuration;
    }

    public SocketAddress getSocketAddress()
    {
        return socketAddress;
    }

    // state

    /**
     * @return the current {@link RaftState} of this raft node
     */
    public RaftState getState()
    {
        return state;
    }

    public LogStream getLogStream()
    {
        return logStream;
    }

    /**
     * @return the current term of this raft node
     */
    public int getTerm()
    {
        return persistentStorage.getTerm();
    }

    /**
     * Update the term of this raft node, resetting the state of the raft for the new term.
     */
    public void setTerm(final int term)
    {
        final int currentTerm = getTerm();

        if (currentTerm < term)
        {
            persistentStorage
                .setTerm(term)
                .setVotedFor(null)
                .save();
        }
        else if (currentTerm > term)
        {
            LOG.debug("Cannot set term to smaller value {} < {}", term, currentTerm);
        }
    }

    /**
     * Checks if the raft term is still current, otherwise step down to become a follower
     * and update the current term.
     *
     * @return true if the current term is updated, false otherwise
     */
    public boolean mayStepDown(final HasTerm hasTerm)
    {
        final int messageTerm = hasTerm.getTerm();
        final int currentTerm = getTerm();

        if (currentTerm < messageTerm)
        {
            LOG.debug("Received message with higher term {} > {}", hasTerm.getTerm(), currentTerm);
            setTerm(messageTerm);
            becomeFollower();

            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * @return true if the message term is greater or equals of the current term
     */
    public boolean isTermCurrent(final HasTerm message)
    {
        return message.getTerm() >= getTerm();
    }

    /**
     * @return the raft which this node voted for in the current term, or null if not voted yet in the current term
     */
    public SocketAddress getVotedFor()
    {
        return persistentStorage.getVotedFor();
    }

    /**
     * @return true if not voted yet in the term or already vote the provided raft node
     */
    public boolean canVoteFor(final HasSocketAddress hasSocketAddress)
    {
        final SocketAddress votedFor = getVotedFor();
        return votedFor == null || votedFor.equals(hasSocketAddress.getSocketAddress());
    }

    /**
     * Set the raft which was granted a vote in the current term.
     */
    public void setVotedFor(final SocketAddress votedFor)
    {
        persistentStorage.setVotedFor(votedFor).save();
    }

    public RaftMembers getRaftMembers()
    {
        return raftMembers;
    }

    public int getMemberSize()
    {
        return raftMembers.getMemberSize();
    }

    /**
     * Replace existing members know by this node with new list of members
     */
    public void replaceMembersOnConfigurationChange(final ValueArray<RaftConfigurationEventMember> members)
    {
        raftMembers.replaceMembersOnConfigurationChange(members);
        notifyMembersChangedListeners();
    }

    /**
     * <p>
     * Add a list of raft nodes to the list of members known by this node if its not already
     * part of the members list.
     * </p>
     *
     * <p>
     * <b>Note:</b> If this node is part of the members list provided it will be ignored and not added to
     * the known members. This would distort the quorum determination.
     * </p>
     */
    public void addMembersWhenJoined(final List<SocketAddress> members)
    {
        raftMembers.addMembersWhenJoined(members);
        notifyMembersChangedListeners();
    }

    /**
     *  Add raft to list of known members of this node and starts the {@link AppendRaftEventController} to write the new configuration to the log stream
     * @return
     */
    public boolean joinMember(final SocketAddress socketAddress)
    {
        LOG.debug("New member {} joining the cluster", socketAddress);
        final RaftMember newMember = raftMembers.addMember(socketAddress);

        if (newMember != null &&  state == RaftState.LEADER)
        {
            // start replication
            final int term = getTerm();
            final ServiceName<RaftState> leaderServiceName = leaderServiceName(raftName, term);
            final ServiceName<Void> replicateLogControllerServiceName = replicateLogConrollerServiceName(raftName, term, newMember.getRemoteAddress().getAddress());

            serviceContext.createService(replicateLogControllerServiceName, new MemberReplicateLogController(this, newMember, clientTransport))
                .dependency(leaderServiceName)
                .install();

            notifyMembersChangedListeners();

            return true;
        }
        return false;
    }

    public boolean leaveMember(final SocketAddress socketAddress)
    {
        LOG.debug("Member {} leaving the cluster", socketAddress);
        final RaftMember removedMemeber = raftMembers.removeMember(socketAddress);

        if (removedMemeber != null)
        {
            persistentStorage.save();
            // stop replication
            serviceContext.removeService(replicateLogConrollerServiceName(raftName, getTerm(), socketAddress));

            notifyMembersChangedListeners();

            return true;
        }
        return false;
    }

    /**
     * @return the number which is required to reach a quorum based on the currently known members
     */
    public int requiredQuorum()
    {
        return RaftMath.getRequiredQuorum(getMemberSize() + 1);
    }

    /**
     * @return true if the partition id of the log stream matches the argument, false otherwise
     */
    public boolean matchesLog(final HasPartition hasPartition)
    {
        return logStream.getPartitionId() == hasPartition.getPartitionId();
    }

    // transport message sending

    /**
     * Send a {@link TransportMessage} to the given remote
     *
     * @return true if the message was written to the send buffer, false otherwise
     */
    public boolean sendMessage(final RemoteAddress remoteAddress, final BufferWriter writer)
    {
        transportMessage
            .reset()
            .remoteAddress(remoteAddress)
            .writer(writer);

        return clientTransport.getOutput().sendMessage(transportMessage);
    }

    /**
     * Send a {@link TransportMessage} to the given address
     *
     * @return true if the message was written to the send buffer, false otherwise
     */
    public void sendMessage(final SocketAddress socketAddress, final BufferWriter writer)
    {
        final RaftMember member = raftMembers.getMemberBySocketAddress(socketAddress);

        final RemoteAddress remoteAddress;
        if (member != null)
        {
            remoteAddress = member.getRemoteAddress();
        }
        else
        {
            remoteAddress = clientTransport.registerRemoteAddress(socketAddress);
        }

        sendMessage(remoteAddress, writer);
    }

    /**
     * Send a request to the given address
     *
     * @return the client request to poll for a response, or null if the request could not be written at the moment
     */
    public ActorFuture<ClientResponse> sendRequest(final RemoteAddress remoteAddress, final BufferWriter writer, Duration timeout)
    {
        return clientTransport.getOutput().sendRequest(remoteAddress, writer, timeout);
    }

    /**
     * Send a response over the given output to the given address
     *
     * @return true if the message was written to the send buffer, false otherwise
     */
    public void sendResponse(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId, final BufferWriter writer)
    {
        serverResponse
            .reset()
            .remoteAddress(remoteAddress)
            .requestId(requestId)
            .writer(writer);

        serverOutput.sendResponse(serverResponse);
    }

    @Override
    public String toString()
    {
        return "raft-" + logStream.getLogName() + "-" + socketAddress.host() + ":" + socketAddress.port();
    }

    /**
     * TODO: needed for testing(?)
     */
    public ActorFuture<Void> clearReceiveBuffer()
    {
        return actor.call(() ->
        {
            messageReceiveBuffer.read((msgTypeId, buffer, index, length) ->
            {
                // discard
            });
        });
    }

    public int getPartitionId()
    {
        return logStream.getPartitionId();
    }

    public DirectBuffer getTopicName()
    {
        return logStream.getTopicName();
    }

    @Override
    public Raft get()
    {
        return this;
    }

    public ConcurrentQueueChannel<IncomingRaftRequest> getRequestQueue()
    {
        return requestQueue;
    }

    public OneToOneRingBufferChannel getMessageReceiveBuffer()
    {
        return messageReceiveBuffer;
    }

    @Override
    public String getName()
    {
        return actorName;
    }

    public Heartbeat getHeartbeat()
    {
        return heartbeat;
    }

    public Injector<LogStream> getLogStreamInjector()
    {
        return logStreamInjector;
    }
}
