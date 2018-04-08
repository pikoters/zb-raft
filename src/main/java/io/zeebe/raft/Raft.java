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

import io.zeebe.logstreams.impl.LogStorageAppender;
import static io.zeebe.raft.RaftServiceNames.*;
import static io.zeebe.util.EnsureUtil.ensureNotNull;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

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
import io.zeebe.util.sched.*;
import io.zeebe.util.sched.channel.ConcurrentQueueChannel;
import io.zeebe.util.sched.channel.OneToOneRingBufferChannel;
import io.zeebe.util.sched.clock.ActorClock;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;

import static io.zeebe.util.EnsureUtil.ensureNotNull;

/**
 * <p>
 * Representation of a member of a raft cluster. It handle three concerns of the
 * raft member:
 * </p>
 *
 * <ul>
 *     <li>holding and updating the raft state</li>
 *     <li>handling raft protocol messages</li>
 *     <li>advancing the work on raft concerns, i.e. replicating the log, triggering elections etc.</li>
 * </ul>
 *
 */
public class Raft extends Actor implements ServerMessageHandler, ServerRequestHandler, Service<Raft>
{
    private static final Logger LOG = Loggers.RAFT_LOGGER;
    private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();

    private final ConcurrentQueueChannel<IncomingRaftRequest> requestQueue = new ConcurrentQueueChannel<>(new ManyToOneConcurrentLinkedQueue<>());

    // environment
    private final OneToOneRingBufferChannel messageReceiveBuffer;
    private final RaftConfiguration configuration;
    private final SocketAddress socketAddress;
    private final ClientTransport clientTransport;

    private ServiceName<?> currentStateServiceName;
    private RaftState state;
    private JoinRaftController joinController;

    // persistent state
    private final LogStream logStream;
    private final RaftPersistentStorage persistentStorage;

    // volatile state
    private final Map<SocketAddress, RaftMember> memberLookup = new HashMap<>();
    private final List<RaftMember> members = new ArrayList<>();
    private final List<RaftStateListener> raftStateListeners = new ArrayList<>();

    // reused entities
    private final TransportMessage transportMessage = new TransportMessage();
    private final ServerResponse serverResponse = new ServerResponse();

    private ServiceStartContext serviceContext;

    private String actorName;
    private String raftName;

    public Raft(final ServiceContainer serviceContainer,
            final RaftConfiguration configuration,
            final SocketAddress socketAddress,
            final LogStream logStream,
            final ClientTransport clientTransport,
            final RaftPersistentStorage persistentStorage,
            final OneToOneRingBufferChannel messageReceiveBuffer,
            final RaftStateListener... listeners)
    {
        this.configuration = configuration;
        this.socketAddress = socketAddress;
        this.logStream = logStream;
        this.clientTransport = clientTransport;
        this.persistentStorage = persistentStorage;
        this.messageReceiveBuffer = messageReceiveBuffer;
        actorName = String.format("%s.%s", logStream.getLogName(), socketAddress.toString());
        raftName = logStream.getLogName();

        raftStateListeners.addAll(Arrays.asList(listeners));

        LOG.info("Created raft with configuration: " + this.configuration);
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        this.serviceContext = startContext;
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

        this.joinController = new JoinRaftController(this, actor);
        final ActorFuture<Void> whenJoined = serviceContext.createService(joinServiceName(raftName), joinController)
            .install();

        actor.runOnCompletion(whenJoined, (v, t) ->
        {
            notifyRaftStateListeners();
        });
    }

    // state transitions

    public void becomeFollower()
    {
        final ActorFuture<Void> prevStateLeft;

        if (currentStateServiceName != null)
        {
            prevStateLeft = serviceContext.removeService(currentStateServiceName);
        }
        else
        {
            prevStateLeft = CompletableActorFuture.completed(null);
        }

        actor.runOnCompletion(prevStateLeft, (v, t) ->
        {
            final ServiceName<RaftState> followerServiceName = followerServiceName(raftName, getTerm());
            final FollowerState followerState = new FollowerState(this, actor);

            actor.runOnCompletion(serviceContext.createService(followerServiceName, followerState).install(), (v2, t2) ->
            {
                if (t2 == null)
                {
                    this.state = RaftState.FOLLOWER;
                    notifyRaftStateListeners();
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
                    this.state = RaftState.CANDIDATE;
                    notifyRaftStateListeners();
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
            final ServiceName<Void> initialEventAppendedServiceName = leaderInitialEventAppendedServiceName(raftName, term);
            final ServiceName<Void> openLogStreamServiceName = leaderOpenLogStreamServiceName(raftName, term);

            final CompositeServiceBuilder installOperation = serviceContext.createComposite(installOperationServiceName);

            installOperation.createService(leaderServiceName, new LeaderState(this, actor))
                .dependency(joinServiceName(raftName))
                .install();
            installOperation.createService(openLogStreamServiceName, new LeaderOpenLogStreamAppenderService(logStream))
                .install();
            installOperation.createService(initialEventAppendedServiceName, new LeaderCommitInitialEvent(this, actor))
                .dependency(openLogStreamServiceName)
                .install();

            for (RaftMember raftMember : members)
            {
                final ServiceName<Void> replicateLogControllerServiceName = replicateLogConrollerServiceName(raftName, term, raftMember.getRemoteAddress().getAddress());

                installOperation.createService(replicateLogControllerServiceName, new MemberReplicateLogController(this, raftMember, clientTransport))
                    .dependency(leaderServiceName)
                    .install();
            }

            final ActorFuture<Void> whenLeader = installOperation.install();

            actor.runOnCompletion(whenLeader, (v2, t2) ->
            {
                if (t == null)
                {
                    this.state = RaftState.LEADER;
                    notifyMemberChangeListeners();
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
        if (joinController.isJoined())
        {
            raftStateListeners.forEach((l) -> LogUtil.catchAndLog(LOG, () ->
                l.onStateChange(this, state)));
        }
    }

    private void notifyMemberChangeListeners()
    {
        if (joinController.isJoined())
        {
            final List<SocketAddress> memberAddresses = members.stream()
                .map((rm) -> rm.getRemoteAddress().getAddress())
                .collect(Collectors.toList());

            raftStateListeners.forEach((l) -> LogUtil.catchAndLog(LOG, () ->
                l.onMembersChanged(this, memberAddresses)));
        }
    }

    public ActorFuture<Void> leave()
    {
        final CompletableActorFuture<Void> leaveFuture = new CompletableActorFuture<>();
        actor.call(() ->
        {
            if (state != leaderState)
            {
                configurationController.leave(leaveFuture);
            }
            else
            {
                leaveFuture.completeExceptionally(new UnsupportedOperationException("Can't leave as leader."));
            }
        });
        return leaveFuture;
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

    public long getLastHeartBeatTime()
    {
        return lastHeartBeatTime;
    }

    public boolean shouldElect()
    {
        final long currentTime = ActorClock.currentTimeMillis();
        return currentTime >= (lastHeartBeatTime + configuration.electionIntervalMs);
    }

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

    /**
     * @return the number of members known by this node, excluding itself
     */
    public int getMemberSize()
    {
        return members.size();
    }

    /**
     * @return the list of members known by this node, excluding itself
     */
    public List<RaftMember> getMembers()
    {
        return members;
    }

    public RaftMember getMember(final int index)
    {
        return members.get(index);
    }

    public RaftMember getMember(final SocketAddress socketAddress)
    {
        return memberLookup.get(socketAddress);
    }

    /**
     * @return true if the raft is know as member by this node, false otherwise
     */
    public boolean isMember(final SocketAddress socketAddress)
    {
        return memberLookup.get(socketAddress) != null;
    }

    /**
     * Replace existing members know by this node with new list of members
     */
    public void setMembers(final ValueArray<RaftConfigurationEventMember> members)
    {
        this.members.clear();
        this.memberLookup.clear();
        persistentStorage.clearMembers();

        final Iterator<RaftConfigurationEventMember> iterator = members.iterator();
        while (iterator.hasNext())
        {
            addMember(iterator.next().getSocketAddress(), false);
        }

        persistentStorage.save();

        notifyMemberChangeListeners();
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
    public void addMembers(final List<SocketAddress> members)
    {
        for (int i = 0; i < members.size(); i++)
        {
            addMember(members.get(i), false);
        }

        persistentStorage.save();

        notifyMemberChangeListeners();
    }

    /**
     *
     * @param socketAddress the address of the new member, the object is stored so it cannot be reused
     * @param b
     */
    private void addMember(final SocketAddress socketAddress, boolean startReplcation)
    {
        ensureNotNull("Raft node socket address", socketAddress);

        if (socketAddress.equals(this.socketAddress))
        {
            return;
        }

        RaftMember member = getMember(socketAddress);

        if (member == null)
        {
            final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(socketAddress);

            member = new RaftMember(remoteAddress, logStream);

            members.add(member);
            memberLookup.put(socketAddress, member);

            persistentStorage.addMember(socketAddress);

            if (startReplcation)
            {
                final int term = getTerm();
                final ServiceName<RaftState> leaderServiceName = leaderServiceName(raftName, term);
                final ServiceName<Void> replicateLogControllerServiceName = replicateLogConrollerServiceName(raftName, term, member.getRemoteAddress().getAddress());

                serviceContext.createService(replicateLogControllerServiceName, new MemberReplicateLogController(this, member, clientTransport))
                    .dependency(leaderServiceName)
                    .install();
            }
        }

    }

    private void removeMember(final SocketAddress socketAddress)
    {
        ensureNotNull("Raft node socket address", socketAddress);

        if (socketAddress.equals(this.socketAddress))
        {
            return;
        }

        final RaftMember member = getMember(socketAddress);
        if (member != null)
        {
            members.remove(member);
            memberLookup.remove(socketAddress, member);

            persistentStorage.removeMember(socketAddress);

            if (getState() == RaftState.LEADER)
            {
                member.stopReplicationController();
            }
        }

    }

    /**
     *  Add raft to list of known members of this node and starts the {@link AppendRaftEventController} to write the new configuration to the log stream
     */
    public void joinMember(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId, final SocketAddress socketAddress)
    {
        LOG.debug("New member {} joining the cluster", socketAddress);
        addMember(socketAddress, true);
        persistentStorage.save();

        final ServiceName<RaftState> leaderServiceName = leaderServiceName(raftName, getTerm());
        final ServiceName<Void> appendRaftEventControllerServiceName = appendRaftEventControllerServiceName(raftName, getTerm(), socketAddress);

        final AppendRaftEventController appendRaftEventController = new AppendRaftEventController(this,
            actor,
            serverOutput,
            remoteAddress,
            requestId);

        serviceContext.createService(appendRaftEventControllerServiceName, appendRaftEventController)
            .dependency(leaderServiceName)
            .install();
    }

    public void removeMember(final ServerOutput serverOutput, final RemoteAddress remoteAddress, final long requestId, final SocketAddress socketAddress)
    {
        LOG.debug("Member {} leaving the cluster", socketAddress);
        removeMember(socketAddress);
        persistentStorage.save();

        appendRaftEventController.appendEvent(serverOutput, remoteAddress, requestId);
    }

    /**
     * @return the number which is required to reach a quorum based on the currently known members
     */
    public int requiredQuorum()
    {
        return Math.floorDiv(members.size() + 1, 2) + 1;
    }

    /**
     * @return true if the log storage appender is currently appendEvent, false otherwise
     */
    public boolean isLogStorageAppenderOpen()
    {
        return logStream.getLogStorageAppender() == null;
    }

    /**
     * @return the position of the initial event of the term, -1 if the event is not written to the log yet
     */
    public long getInitialEventPosition()
    {
        return openLogStreamController.getPosition();
    }

    /**
     * @return true if the initial event of this node for the current term was committed to the log stream, false otherwise
     */
    public boolean isInitialEventCommitted()
    {
        return openLogStreamController.isPositionCommited();
    }

    /**
     * @return true if the last raft configuration event created by this node was committed to the log stream, false otherwise
     */
    public boolean isConfigurationEventCommitted()
    {
        return appendRaftEventController.isCommitted();
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
        final RaftMember member = memberLookup.get(socketAddress);

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

    public boolean isJoined()
    {
        return configurationController.isJoined();
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
        return getTopicName();
    }

    public int getReplicationFactor()
    {
        return replicationFactor;
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
}
