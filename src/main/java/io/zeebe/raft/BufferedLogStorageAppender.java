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

import static io.zeebe.raft.AppendRequestEncoder.previousEventPositionNullValue;
import static io.zeebe.raft.AppendRequestEncoder.previousEventTermNullValue;

import java.nio.ByteBuffer;

import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.raft.event.RaftConfiguration;
import io.zeebe.raft.protocol.AppendRequest;
import io.zeebe.raft.protocol.AppendResponse;
import io.zeebe.util.allocation.AllocatedBuffer;
import io.zeebe.util.allocation.DirectBufferAllocator;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

public class BufferedLogStorageAppender
{

    public static final int INITIAL_CAPACITY = 32 * 1024;

    private final DirectBufferAllocator allocator = new DirectBufferAllocator();
    private final BrokerEventMetadata metadata = new BrokerEventMetadata();
    private final RaftConfiguration configuration = new RaftConfiguration();
    private final AppendResponse appendResponse = new AppendResponse();

    private final Raft raft;
    private final Logger logger;
    private final LogStream logStream;
    private final BufferedLogStreamReader reader;

    // event buffer and offset
    private final MutableDirectBuffer buffer;
    private int offset;

    // last event written to log storage
    private long lastWrittenPosition;
    private int lastWrittenTerm;

    // last event added to buffer
    private long lastBufferedPosition;
    private int lastBufferedTerm;

    public BufferedLogStorageAppender(final Raft raft)
    {
        this.raft = raft;
        this.logger = raft.getLogger();
        this.logStream = raft.getLogStream();
        this.reader = new BufferedLogStreamReader(logStream, true);

        final AllocatedBuffer allocatedBuffer = allocator.allocate(INITIAL_CAPACITY);
        buffer = new UnsafeBuffer(allocatedBuffer.getRawBuffer());
        offset = 0;
    }

    public void reset()
    {
        reader.seekToLastEvent();

        if (reader.hasNext())
        {
            final LoggedEvent lastEvent = reader.next();
            lastEvent.readMetadata(metadata);

            lastWrittenPosition = lastEvent.getPosition();
            lastWrittenTerm = metadata.getRaftTermId();
        }
        else
        {
            lastWrittenPosition = previousEventPositionNullValue();
            lastWrittenTerm = previousEventTermNullValue();
        }

        discardBufferedEvents();
    }

    public boolean isLastEvent(final long position, final int term)
    {
        return (lastBufferedPosition == position && lastBufferedTerm == term) || (lastWrittenPosition == position && lastWrittenTerm == term);
    }

    public boolean isAfterOrEqualsLastEvent(final long position, final int term)
    {
        return term > lastBufferedTerm || (term == lastBufferedTerm && position >= lastBufferedPosition);
    }

    public long getPreviousPosition(final long position, final int term)
    {
        final long commitPosition = logStream.getCommitPosition();

        if (position == lastBufferedPosition)
        {
            return lastBufferedPosition;
        }
        else if (position == lastWrittenPosition)
        {
            return lastWrittenPosition;
        }
        else if (position > lastWrittenPosition)
        {
            return lastWrittenPosition;
        }
        else if (position > commitPosition)
        {
            return position;
        }
        else
        {
            return commitPosition;
        }
    }

    public void appendEvent(final AppendRequest appendRequest, final LoggedEventImpl event)
    {
        if (event != null)
        {
            final long previousPosition = appendRequest.getPreviousEventPosition();
            final long previousTerm = appendRequest.getPreviousEventTerm();

            if (previousPosition == lastWrittenPosition && previousTerm == lastWrittenTerm)
            {
                discardBufferedEvents();
            }

            if (previousPosition == lastBufferedPosition && previousTerm == lastBufferedTerm)
            {
                final int eventLength = event.getFragmentLength();
                if (remainingCapacity() < eventLength)
                {
                    if (!flushBufferedEvents())
                    {
                        // unable to flush events, abort and try again with last buffered position
                        rejectAppendRequest(appendRequest, lastBufferedPosition);
                    }
                }

                if (remainingCapacity() < eventLength)
                {
                    allocateMemory(eventLength);
                }

                buffer.putBytes(offset, event.getBuffer(), event.getFragmentOffset(), eventLength);
                offset += eventLength;

                event.readMetadata(metadata);

                lastBufferedPosition = event.getPosition();
                lastBufferedTerm = metadata.getRaftTermId();

                if (metadata.getEventType() == EventType.RAFT_EVENT)
                {
                    // update configuration
                    event.readValue(configuration);
                    raft.setMembers(configuration.members());
                }
            }
            else
            {
                logger.warn("Event to append does not follow previous event {}/{} != {}/{}", lastBufferedPosition, lastBufferedTerm, previousPosition,
                    previousTerm);
            }
        }

        acceptAppendRequest(appendRequest, lastWrittenPosition);
    }

    public void truncateLog(final AppendRequest appendRequest, final LoggedEventImpl event)
    {
        final long currentCommit = logStream.getCommitPosition();

        final long previousEventPosition = appendRequest.getPreviousEventPosition();
        final int previousEventTerm = appendRequest.getPreviousEventTerm();

        if (previousEventPosition >= lastBufferedPosition || raft.isLogStreamControllerOpen())
        {
            // event is either after our last position or the log stream controller
            // is still open, which does not allow to truncate the log
            rejectAppendRequest(appendRequest, lastBufferedPosition);
        }
        else if (previousEventPosition < currentCommit)
        {
            rejectAppendRequest(appendRequest, currentCommit);
        }
        else if (reader.seek(previousEventPosition) && reader.hasNext())
        {
            final LoggedEvent writtenEvent = reader.next();
            writtenEvent.readMetadata(metadata);

            if (writtenEvent.getPosition() == previousEventPosition && metadata.getRaftTermId() == previousEventTerm)
            {
                truncateLogAfter(writtenEvent);
                appendEvent(appendRequest, event);
            }
            else
            {
                rejectAppendRequest(appendRequest, writtenEvent.getPosition() - 1);
            }
        }
        else
        {
            rejectAppendRequest(appendRequest, lastWrittenPosition);
        }
    }

    private void truncateLogAfter(final LoggedEvent event)
    {
        if (reader.hasNext())
        {
            final LoggedEvent nextEvent = reader.next();
            logStream.truncate(nextEvent.getPosition());
        }

        event.readMetadata(metadata);

        lastWrittenPosition = event.getPosition();
        lastWrittenTerm = metadata.getRaftTermId();

        lastBufferedPosition = lastWrittenPosition;
        lastBufferedTerm = lastWrittenTerm;
    }

    private void allocateMemory(final int capacity)
    {
        discardBufferedEvents();
        final AllocatedBuffer allocatedBuffer = allocator.allocate(capacity);
        buffer.wrap(allocatedBuffer.getRawBuffer());
    }

    private int remainingCapacity()
    {
        return buffer.capacity() - offset;
    }

    private void discardBufferedEvents()
    {
        buffer.setMemory(0, offset, (byte) 0);
        offset = 0;

        lastBufferedPosition = lastWrittenPosition;
        lastBufferedTerm = lastWrittenTerm;
    }

    public boolean flushBufferedEvents()
    {
        if (offset > 0)
        {
            final ByteBuffer byteBuffer = buffer.byteBuffer();
            byteBuffer.position(0);
            byteBuffer.limit(offset);

            final long address = logStream.getLogStorage().append(byteBuffer);

            if (address >= 0)
            {
                lastWrittenPosition = lastBufferedPosition;
                lastWrittenTerm = lastBufferedTerm;

                discardBufferedEvents();
                return true;
            }
            else
            {
                byteBuffer.clear();
                return false;
            }
        }

        return true;
    }

    protected void acceptAppendRequest(final AppendRequest appendRequest, final long position)
    {

        final long currentCommitPosition = logStream.getCommitPosition();
        final long nextCommitPosition = Math.min(position, appendRequest.getCommitPosition());

        if (nextCommitPosition >= 0 && nextCommitPosition > currentCommitPosition)
        {
            logger.debug("Committing position {} as {}", nextCommitPosition, raft.getState());
            logStream.setCommitPosition(nextCommitPosition);
        }

        appendResponse
            .reset()
            .setRaft(raft)
            .setPreviousEventPosition(position)
            .setSucceeded(true);

        raft.sendMessage(appendRequest.getSocketAddress(), appendResponse);
    }

    protected void rejectAppendRequest(final AppendRequest appendRequest, final long position)
    {
        appendResponse
            .reset()
            .setRaft(raft)
            .setPreviousEventPosition(position)
            .setSucceeded(false);

        raft.sendMessage(appendRequest.getSocketAddress(), appendResponse);
    }

    public long getLastPosition()
    {
        return lastBufferedPosition;
    }
}
