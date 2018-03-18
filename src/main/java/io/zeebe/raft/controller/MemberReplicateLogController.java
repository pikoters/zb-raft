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

import static io.zeebe.raft.AppendRequestEncoder.previousEventPositionNullValue;
import static io.zeebe.raft.AppendRequestEncoder.previousEventTermNullValue;

import java.time.Duration;

import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.impl.log.index.LogBlockIndex;
import io.zeebe.logstreams.log.*;
import io.zeebe.raft.*;
import io.zeebe.raft.Loggers;
import io.zeebe.raft.protocol.AppendRequest;
import io.zeebe.transport.*;
import io.zeebe.util.collection.LongRingBuffer;
import io.zeebe.util.sched.*;
import io.zeebe.util.sched.clock.ActorClock;
import io.zeebe.util.sched.future.ActorFuture;
import org.slf4j.Logger;

/**
 * Per-follower replication controller
 */
public class MemberReplicateLogController extends Actor
{
    private static final Logger LOG = Loggers.RAFT_LOGGER;
    private static final boolean IS_TRACE_ENABLED = LOG.isTraceEnabled();

    private final AppendRequest appendRequest = new AppendRequest();
    private final TransportMessage transportMessage = new TransportMessage();

    private final LongRingBuffer unacknowledgedEventPositions = new LongRingBuffer(512);
    private long lastRequestTimestamp;

    final Runnable sendNextEventsFn = this::sendNextEvents;

    private final Raft raft;
    private final LogStream logStream;
    private final long heartbeatIntervalMs;
    private final RemoteAddress remoteAddress;
    private final ClientOutput clientOutput;

    private final BufferedLogStreamReader reader;
    private LoggedEventImpl bufferedEvent;
    private long previousPosition;
    private int previousTerm;

    private ActorCondition appenderCondition;
    private final String name;

    public MemberReplicateLogController(Raft raft, RaftMember member, ClientTransport clientTransport)
    {
        this.remoteAddress = member.getRemoteAddress();
        this.name = String.format("raft-repl-%s-%s", raft.getName(), remoteAddress.toString());

        this.raft = raft;
        this.heartbeatIntervalMs = raft.getConfiguration().getHeartbeatIntervalMs();
        this.clientOutput = clientTransport.getOutput();
        this.logStream = raft.getLogStream();
        this.reader = new BufferedLogStreamReader(logStream, true);
    }

    @Override
    public String getName()
    {
        return name;
    }

    public ActorFuture<Void> close()
    {
        return actor.close();
    }

    @Override
    protected void onActorStarted()
    {
        if (IS_TRACE_ENABLED)
        {
            LOG.trace("started");
        }

        actor.runAtFixedRate(Duration.ofMillis(heartbeatIntervalMs), this::onHeartbeatTimerFired);
        appenderCondition = actor.onCondition("data-appended", this::onAppendPositionChanged);
        raft.getLogStream().registerOnAppendCondition(appenderCondition);

        reset();
    }

    @Override
    protected void onActorClosing()
    {
        raft.getLogStream().removeOnCommitPositionUpdatedCondition(appenderCondition);
    }

    @Override
    protected void onActorClosed()
    {
        if (IS_TRACE_ENABLED)
        {
            LOG.trace("closed");
        }

        reader.close();
    }

    private void onHeartbeatTimerFired()
    {
        if (IS_TRACE_ENABLED)
        {
            LOG.trace("heartbeat timer fired");
        }

        actor.runUntilDone(sendNextEventsFn);
    }

    private void onAppendPositionChanged()
    {
        if (IS_TRACE_ENABLED)
        {
            LOG.trace("events appended");
        }

        actor.runUntilDone(sendNextEventsFn);
    }

    public void onFollowerHasAcknowledgedPosition(long position)
    {
        actor.run(() ->
        {
            if (IS_TRACE_ENABLED)
            {
                LOG.trace("follower acknowledged position {}", position);
            }

            unacknowledgedEventPositions.consumeAscendingUntilInclusive(position);
            actor.runUntilDone(sendNextEventsFn);
        });
    }

    public void onFollowerHasFailedPosition(long position)
    {
        actor.run(() ->
        {
            if (IS_TRACE_ENABLED)
            {
                LOG.trace("follower failed position {}", position);
            }
            resetToPosition(position);
            unacknowledgedEventPositions.consumeAscendingUntilInclusive(Long.MAX_VALUE);
            actor.runUntilDone(sendNextEventsFn);
        });
    }

    private void sendNextEvents()
    {
        if (IS_TRACE_ENABLED)
        {
            LOG.trace("try send next event to {}", remoteAddress);
        }

        actor.setPriority(ActorPriority.REGULAR);

        final long now = ActorClock.currentTimeMillis();
        final boolean mustSendHeartbeatNow = now - lastRequestTimestamp >= heartbeatIntervalMs;

        if (!mustSendHeartbeatNow && (!hasNextEvent() || unacknowledgedEventPositions.isSaturated()))
        {
            actor.done();
        }
        else
        {
            final LoggedEventImpl nextEvent = getNextEvent();
            if (trySendEvent(nextEvent))
            {
                lastRequestTimestamp = now;

                if (nextEvent != null)
                {
                    if (IS_TRACE_ENABLED)
                    {
                        LOG.trace("sucessfully sent event with pos {}", nextEvent.getPosition());
                    }

                    unacknowledgedEventPositions.addElementToHead(nextEvent.getPosition());
                    setPreviousEvent(nextEvent);
                }
                else
                {
                    if (IS_TRACE_ENABLED)
                    {
                        LOG.trace("sucessfully sent empty heartbeat");
                    }

                    actor.done(); // empty heartbeat
                }
            }
            else
            {
                setBufferedEvent(nextEvent);

                if (mustSendHeartbeatNow)
                {
                    actor.setPriority(ActorPriority.HIGH);
                }
                else
                {
                    actor.setPriority(ActorPriority.LOW);
                }

                actor.yield();
            }
        }
    }

    private boolean trySendEvent(LoggedEventImpl event)
    {
        appendRequest.reset()
            .setRaft(raft)
            .setPreviousEventPosition(previousPosition)
            .setPreviousEventTerm(previousTerm)
            .setEvent(event);

        transportMessage.reset()
            .remoteAddress(remoteAddress)
            .writer(appendRequest);

        return clientOutput.sendMessage(transportMessage);
    }

    private void setBufferedEvent(final LoggedEventImpl bufferedEvent)
    {
        this.bufferedEvent = bufferedEvent;
    }

    private LoggedEventImpl discardBufferedEvent()
    {
        final LoggedEventImpl event = bufferedEvent;
        bufferedEvent = null;
        return event;
    }

    private void reset()
    {
        setPreviousEventToEndOfLog();
    }

    private LoggedEventImpl getNextEvent()
    {
        if (bufferedEvent != null)
        {
            return discardBufferedEvent();
        }
        else if (reader.hasNext())
        {
            return (LoggedEventImpl) reader.next();
        }
        else
        {
            return null;
        }
    }

    private void resetToPosition(final long eventPosition)
    {
        if (eventPosition >= 0)
        {
            final LoggedEvent previousEvent = getEventAtPosition(eventPosition);
            if (previousEvent != null)
            {
                setPreviousEvent(previousEvent);
            }
            else
            {
                final LogBlockIndex logBlockIndex = logStream.getLogBlockIndex();
                final long blockPosition = logBlockIndex.lookupBlockPosition(eventPosition);

                if (blockPosition > 0)
                {
                    reader.seek(blockPosition);
                }
                else
                {
                    reader.seekToFirstEvent();
                }

                long previousPosition = -1;

                while (reader.hasNext())
                {
                    final LoggedEvent next = reader.next();

                    if (next.getPosition() < eventPosition)
                    {
                        previousPosition = next.getPosition();
                    }
                    else
                    {
                        break;
                    }
                }

                if (previousPosition >= 0)
                {
                    setPreviousEvent(previousPosition);
                }
                else
                {
                    setPreviousEventToStartOfLog();
                }
            }
        }
        else
        {
            setPreviousEventToStartOfLog();
        }
    }

    private LoggedEvent getEventAtPosition(final long position)
    {
        if (reader.seek(position) && reader.hasNext())
        {
            return reader.next();
        }
        else
        {
            return null;
        }
    }

    private void setPreviousEventToEndOfLog()
    {
        discardBufferedEvent();

        reader.seekToLastEvent();

        final LoggedEventImpl lastEvent = getNextEvent();
        setPreviousEvent(lastEvent);
    }

    private void setPreviousEventToStartOfLog()
    {
        discardBufferedEvent();

        reader.seekToFirstEvent();

        setPreviousEvent(null);
    }

    private void setPreviousEvent(final long previousPosition)
    {
        discardBufferedEvent();

        final LoggedEvent previousEvent = getEventAtPosition(previousPosition);

        setPreviousEvent(previousEvent);
    }

    private void setPreviousEvent(final LoggedEvent previousEvent)
    {
        discardBufferedEvent();

        if (previousEvent != null)
        {
            previousPosition = previousEvent.getPosition();
            previousTerm = previousEvent.getRaftTerm();
        }
        else
        {
            previousPosition = previousEventPositionNullValue();
            previousTerm = previousEventTermNullValue();
        }
    }

    private boolean hasNextEvent()
    {
        return bufferedEvent != null || reader.hasNext();
    }
}
