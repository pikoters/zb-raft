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
package io.zeebe.raft.util;

import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.dispatcher.Dispatchers;
import io.zeebe.dispatcher.FragmentHandler;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.raft.Loggers;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftStateListener;
import io.zeebe.raft.event.RaftConfiguration;
import io.zeebe.raft.event.RaftConfigurationMember;
import io.zeebe.raft.state.RaftState;
import io.zeebe.test.util.TestUtil;
import io.zeebe.transport.BufferingServerTransport;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.SocketAddress;
import io.zeebe.transport.Transports;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import org.agrona.DirectBuffer;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static io.zeebe.raft.state.RaftState.LEADER;
import static io.zeebe.util.buffer.BufferUtil.bufferAsString;
import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

public class RaftRule extends ExternalResource implements RaftStateListener
{

    public static final FragmentHandler NOOP_FRAGMENT_HANDLER = (buffer, offset, length, streamId, isMarkedFailed) -> FragmentHandler.CONSUME_FRAGMENT_RESULT;

    protected final ActorSchedulerRule actorSchedulerRule;
    protected final SocketAddress socketAddress;
    protected final String topicName;
    protected final int partition;
    protected final RaftConfiguration configuration = new RaftConfiguration();
    protected final LogStreamWriterImpl writer = new LogStreamWriterImpl();
    protected final List<RaftRule> members;
    protected final BrokerEventMetadata metadata = new BrokerEventMetadata();

    protected ClientTransport clientTransport;
    protected Dispatcher clientSendBuffer;

    protected Dispatcher serverSendBuffer;
    protected BufferingServerTransport serverTransport;

    protected Dispatcher serverReceiveBuffer;

    protected LogStream logStream;
    protected Raft raft;
    protected BufferedLogStreamReader uncommittedReader;
    protected BufferedLogStreamReader committedReader;

    private InMemoryRaftPersistentStorage persistentStorage;

    protected final List<RaftState> raftStateChanges = new ArrayList<>();

    public RaftRule(final ActorSchedulerRule actorSchedulerRule, final String host, final int port, final String topicName, final int partition, final RaftRule... members)
    {
        this.actorSchedulerRule = actorSchedulerRule;
        this.socketAddress = new SocketAddress(host, port);
        this.topicName = topicName;
        this.partition = partition;
        this.members = members != null ? Arrays.asList(members) : Collections.emptyList();
    }

    @Override
    protected void before() throws Throwable
    {
        init();
        schedule();
    }

    private void init() throws IOException
    {
        final String name = socketAddress.toString();

        serverSendBuffer =
            Dispatchers.create("serverSendBuffer-" + name)
                       .bufferSize(32 * 1024 * 1024)
                       .subscriptions("sender-" + name)
                       .actorScheduler(actorSchedulerRule.get())
                       .build();

        clientSendBuffer =
            Dispatchers.create("clientSendBuffer-" + name)
                       .bufferSize(32 * 1024 * 1024)
                       .subscriptions("sender-" + name)
                       .actorScheduler(actorSchedulerRule.get())
                       .build();

        clientTransport =
            Transports.newClientTransport()
                      .sendBuffer(clientSendBuffer)
                      .requestPoolSize(128)
                      .scheduler(actorSchedulerRule.get())
                      .build();

        logStream =
            LogStreams.createFsLogStream(wrapString(topicName), partition)
                      .deleteOnClose(true)
                      .logDirectory(Files.createTempDirectory("raft-test-" + socketAddress.port() + "-").toString())
                      .logStreamControllerDisabled(true)
                      .actorScheduler(actorSchedulerRule.get())
                      .build();

        logStream.open();

        persistentStorage = new InMemoryRaftPersistentStorage(logStream);

        uncommittedReader = new BufferedLogStreamReader(logStream, true);
        committedReader = new BufferedLogStreamReader(logStream, false);

        setupNewRaft();

        raft.addMembers(members.stream().map(RaftRule::getSocketAddress).collect(Collectors.toList()));
    }

    @Override
    protected void after()
    {
        if (raft != null)
        {
            raft.close();
            raft = null;

            serverTransport.close();

            serverReceiveBuffer.close();
        }

        logStream.close();

        serverSendBuffer.close();

        clientTransport.close();
        clientSendBuffer.close();

        uncommittedReader.close();
        committedReader.close();
    }

    public void schedule()
    {
        actorSchedulerRule.get().submitActor(getRaft());
    }

    public SocketAddress getSocketAddress()
    {
        return socketAddress;
    }

    public LogStream getLogStream()
    {
        return logStream;
    }

    public int getTerm()
    {
        return getRaft().getTerm();
    }

    public RaftState getState()
    {
        return getRaft().getState();
    }

    @Override
    public void onStateChange(final int partitionId, DirectBuffer topicName, final SocketAddress socketAddress, final RaftState raftState)
    {
        assertThat(partitionId).isEqualTo(getRaft().getLogStream().getPartitionId());
        assertThat(topicName).isEqualByComparingTo(getRaft().getLogStream().getTopicName());
        assertThat(socketAddress).isEqualTo(getRaft().getSocketAddress());
        raftStateChanges.add(raftState);
    }

    public List<RaftState> getRaftStateChanges()
    {
        return raftStateChanges;
    }

    public InMemoryRaftPersistentStorage getPersistentStorage()
    {
        return persistentStorage;
    }

    public void clearSubscription()
    {
        final String subscriptionName = getRaft().getSubscriptionName();
        final Subscription subscription = serverReceiveBuffer.getSubscription(subscriptionName);
        subscription.poll(NOOP_FRAGMENT_HANDLER, Integer.MAX_VALUE);
    }

    public boolean isLeader()
    {
        return getState() == LEADER;
    }

    public long writeEvent(final String message)
    {

        final long[] writtenPosition = new long[1];

        writer.wrap(logStream);

        final DirectBuffer value = wrapString(message);

        TestUtil.doRepeatedly(() -> writer.positionAsKey().metadataWriter(metadata.reset()).value(value).tryWrite())
                .until(position ->
                {
                    if (position != null && position >= 0)
                    {
                        writtenPosition[0] = position;
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }, "Failed to write event with message {}", message);

        return writtenPosition[0];
    }

    public long writeEvents(final String... messages)
    {
        long position = 0;

        for (final String message : messages)
        {
            position = writeEvent(message);
        }

        return position;
    }

    public boolean eventAppended(final long position, final int term, final String message)
    {
        uncommittedReader.seek(position);

        if (uncommittedReader.hasNext())
        {
            final LoggedEvent event = uncommittedReader.next();
            final String value = bufferAsString(event.getValueBuffer(), event.getValueOffset(), event.getValueLength());

            return event.getPosition() == position && event.getRaftTerm() == term && message.equals(value);
        }

        return false;
    }

    public boolean eventCommitted(final long position, final int term, final String message)
    {
        final boolean isCommitted = logStream.getCommitPosition() >= position;

        return isCommitted && eventAppended(position, term, message);
    }

    public boolean eventsCommitted(final String... messages)
    {
        committedReader.seekToFirstEvent();

        final BrokerEventMetadata metadata = new BrokerEventMetadata();

        return
            Arrays.stream(messages)
                  .allMatch(message -> {
                      while (committedReader.hasNext())
                      {
                          final LoggedEvent event = committedReader.next();
                          event.readMetadata(metadata);

                          if (metadata.getEventType() == EventType.NULL_VAL)
                          {
                              try
                              {
                                  final String value = bufferAsString(event.getValueBuffer(), event.getValueOffset(), event.getValueLength());

                                  return message.equals(value);
                              }
                              catch (final Exception e)
                              {
                                  // ignore
                              }
                          }
                      }

                      return false;
                  });
    }

    public boolean eventCommitted(final int term, final EventType eventType)
    {
        committedReader.seekToFirstEvent();

        while (committedReader.hasNext())
        {
            final LoggedEvent event = committedReader.next();
            event.readMetadata(metadata);

            if (event.getRaftTerm() == term && metadata.getEventType() == eventType)
            {
                return true;
            }
            else if (event.getRaftTerm() > term)
            {
                // early abort which assumes that terms are always increasing
                return false;
            }
        }

        return false;
    }

    public boolean raftEventCommitted(final int term, final RaftRule... members)
    {
        committedReader.seekToFirstEvent();

        final Set<SocketAddress> expected;

        if (members == null)
        {
            expected = Collections.emptySet();
        }
        else
        {
            expected = Arrays.stream(members).map(RaftRule::getSocketAddress).collect(Collectors.toSet());
        }


        while (committedReader.hasNext())
        {
            final LoggedEvent event = committedReader.next();
            event.readMetadata(metadata);

            if (event.getRaftTerm() == term && metadata.getEventType() == EventType.RAFT_EVENT)
            {
                event.readValue(configuration);

                final Iterator<RaftConfigurationMember> configurationMembers = configuration.members().iterator();

                final Set<SocketAddress> found = new HashSet<>();

                while (configurationMembers.hasNext())
                {
                    final RaftConfigurationMember configurationMember = configurationMembers.next();
                    found.add(configurationMember.getSocketAddress());
                }

                if (expected.equals(found))
                {
                    return true;
                }
            }
            else if (event.getRaftTerm() > term)
            {
                // early abort which assumes that terms are always increasing
                return false;
            }
        }

        return false;
    }

    public void closeRaft()
    {
        raft.close();
        serverTransport.close();
        serverReceiveBuffer.close();
        raft = null;
    }

    public void setupNewRaft()
    {
        Loggers.RAFT_LOGGER.error("SETUP AGAIN");
        final String name = socketAddress.toString();

        serverReceiveBuffer =
            Dispatchers.create("serverReceiveBuffer-" + name)
                .bufferSize(32 * 1024 * 1024)
                .subscriptions("sender-" + name)
                .actorScheduler(actorSchedulerRule.get())
                .build();

        serverTransport =
            Transports.newServerTransport()
                .sendBuffer(serverSendBuffer)
                .bindAddress(socketAddress.toInetSocketAddress())
                .scheduler(actorSchedulerRule.get())
                .buildBuffering(serverReceiveBuffer);

        raft = new Raft(socketAddress, logStream, serverTransport, clientTransport, persistentStorage)
        {
            @Override
            public String getName()
            {
                return socketAddress.toString();
            }
        };
        raft.registerRaftStateListener(this);

    }

    public Raft getRaft()
    {
        if (raft == null)
        {
            try
            {
                setupNewRaft();
            }
            catch (Exception e)
            {
                Loggers.RAFT_LOGGER.error("Ohoh something bad happens here...", e);
            }
        }
        return raft;
    }

    @Override
    public String toString()
    {
        return getRaft().toString();
    }
}
