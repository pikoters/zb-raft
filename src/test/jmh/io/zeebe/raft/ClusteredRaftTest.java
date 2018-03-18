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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import io.zeebe.logstreams.log.*;
import io.zeebe.raft.state.RaftState;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.sched.ActorScheduler;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class ClusteredRaftTest
{
    static final AtomicInteger THREAD_ID = new AtomicInteger(0);
    private static final int BURST_SIZE = 1000;

    private static final MutableDirectBuffer METADATA = new UnsafeBuffer(new byte[31]);
    private static final MutableDirectBuffer DATA = new UnsafeBuffer(new byte[576]);

    @Benchmark
    @Threads(1)
    public void write(BenchmarkContext ctx) throws InterruptedException
    {
        final LogStreamWriter writer = ctx.writer;
        final LogStream logStream = ctx.leader.getLogStream();
        long lastPosition = -1;

        for (int i = 0; i < BURST_SIZE; )
        {
            final long position = writer
                .positionAsKey()
                .metadata(METADATA)
                .value(DATA)
                .tryWrite();

            if (position > 0)
            {
                i++;
                lastPosition = position;
            }
        }

        while (logStream.getCommitPosition() < lastPosition)
        {
            // spin
            Thread.yield();
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkContext
    {
        final ActorScheduler scheduler = ActorScheduler.newActorScheduler()
            .setIoBoundActorThreadCount(3)
            .setCpuBoundActorThreadCount(2)
            .build();

        final ThroughPutTestRaft raft1 = new ThroughPutTestRaft(new SocketAddress("localhost", 51015));
        final ThroughPutTestRaft raft2 = new ThroughPutTestRaft(new SocketAddress("localhost", 51016), raft1);
        final ThroughPutTestRaft raft3 = new ThroughPutTestRaft(new SocketAddress("localhost", 51017), raft1);

        final LogStreamWriter writer = new LogStreamWriterImpl();

        Raft leader;

        @Setup
        public void setUp() throws IOException
        {
            scheduler.start();
            raft1.open(scheduler);
            raft2.open(scheduler);
            raft3.open(scheduler);

            List<Raft> rafts = Arrays.asList(raft1.getRaft(), raft2.getRaft(), raft3.getRaft());

            while (true)
            {
                final Optional<Raft> leader = rafts.stream().filter(r -> r.getState() == RaftState.LEADER).findAny();

                if (leader.isPresent()
                        && rafts.stream().filter(r -> r.getMemberSize() == 2).count() == 3
                        && rafts.stream().filter(r -> r.getState() == RaftState.FOLLOWER).count() == 2)
                {
                    this.leader = leader.get();
                    break;
                }
            }

            writer.wrap(leader.getLogStream());

            System.out.println("Leader: " + leader);
        }

        @TearDown
        public void tearDown() throws InterruptedException, ExecutionException, TimeoutException
        {
            raft1.close();
            raft2.close();
            raft3.close();
            scheduler.stop().get();
        }
    }
}
