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

import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.Raft;
import io.zeebe.raft.event.InitialEvent;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.state.*;
import org.slf4j.Logger;

public class OpenLogStreamController
{

    private static final int TRANSITION_DEFAULT = 0;
    private static final int TRANSITION_FAILED = 1;
    private static final int TRANSITION_OPEN = 2;
    private static final int TRANSITION_CLOSE = 3;

    private static final StateMachineCommand<Context> OPEN_COMMAND = context -> context.take(TRANSITION_OPEN);
    private static final StateMachineCommand<Context> CLOSE_COMMAND = context -> context.take(TRANSITION_CLOSE);

    private final WaitState<Context> committed = context -> { };

    private final StateMachine<Context> stateMachine;
    private final StateMachineAgent<Context> stateMachineAgent;

    public OpenLogStreamController(final Raft raft)
    {
        final State<Context> openLogController = new OpenLogControllerState();
        final State<Context> awaitOpenLogController = new AwaitOpenLogControllerState();
        final State<Context> appendInitialEvent = new AppendInitialEventState();
        final State<Context> awaitInitialEventAppended = new AwaitInitialEventAppendedState();
        final State<Context> awaitInitialEventCommitted = new AwaitInitialEventCommittedState();
        final State<Context> closeLogController = new CloseLogControllerState();
        final State<Context> awaitCloseLogController = new AwaitCloseLogControllerState();
        final State<Context> failedToOpenLogController = new FailedToOpenLogControllerState();
        final WaitState<Context> closed = context -> { };

        stateMachine = StateMachine.<Context>builder(s -> new Context(s, raft))
            .initialState(closed)

            .from(closed).take(TRANSITION_OPEN).to(openLogController)
            .from(closed).take(TRANSITION_CLOSE).to(closed)

            .from(openLogController).take(TRANSITION_DEFAULT).to(awaitOpenLogController)
            .from(openLogController).take(TRANSITION_FAILED).to(failedToOpenLogController)

            .from(awaitOpenLogController).take(TRANSITION_DEFAULT).to(appendInitialEvent)
            .from(awaitOpenLogController).take(TRANSITION_FAILED).to(openLogController)

            .from(appendInitialEvent).take(TRANSITION_DEFAULT).to(awaitInitialEventAppended)
            .from(appendInitialEvent).take(TRANSITION_FAILED).to(appendInitialEvent)

            .from(awaitInitialEventAppended).take(TRANSITION_DEFAULT).to(awaitInitialEventCommitted)
            .from(awaitInitialEventAppended).take(TRANSITION_FAILED).to(appendInitialEvent)
            .from(awaitInitialEventAppended).take(TRANSITION_OPEN).to(awaitInitialEventAppended)
            .from(awaitInitialEventAppended).take(TRANSITION_CLOSE).to(closeLogController)

            .from(awaitInitialEventCommitted).take(TRANSITION_DEFAULT).to(committed)
            .from(awaitInitialEventCommitted).take(TRANSITION_OPEN).to(awaitInitialEventCommitted)
            .from(awaitInitialEventCommitted).take(TRANSITION_CLOSE).to(closeLogController)

            .from(committed).take(TRANSITION_OPEN).to(committed)
            .from(committed).take(TRANSITION_CLOSE).to(closeLogController)

            .from(closeLogController).take(TRANSITION_DEFAULT).to(awaitCloseLogController)
            .from(awaitCloseLogController).take(TRANSITION_DEFAULT).to(closed)

            .from(failedToOpenLogController).take(TRANSITION_DEFAULT).to(closed)

            .build();

        stateMachineAgent = new LimitedStateMachineAgent<>(stateMachine);
    }

    public int doWork()
    {
        return stateMachineAgent.doWork();
    }

    public void reset()
    {
        stateMachineAgent.reset();
    }

    public void open()
    {
        stateMachineAgent.addCommand(OPEN_COMMAND);
    }

    public void close()
    {
        stateMachineAgent.addCommand(CLOSE_COMMAND);
    }

    public long getInitialEventPosition()
    {
        return stateMachine.getContext().getPosition();
    }

    public boolean isCommitted()
    {
        return stateMachineAgent.getCurrentState() == committed;
    }

    static class OpenLogControllerState implements State<Context>
    {

        @Override
        public int doWork(final Context context) throws Exception
        {
            if (context.hasRetriesLeft())
            {
                final LogStream logStream = context.getRaft().getLogStream();

                context.setFuture(logStream.openLogStreamController());
                context.take(TRANSITION_DEFAULT);
            }
            else
            {
                context.take(TRANSITION_FAILED);
            }

            return 1;
        }

        @Override
        public boolean isInterruptable()
        {
            return false;
        }

    }

    static class AwaitOpenLogControllerState implements State<Context>
    {

        @Override
        public int doWork(final Context context) throws Exception
        {
            int workCount = 0;

            final Logger logger = context.getRaft().getLogger();
            final ActorFuture<Void> future = context.getFuture();

            if (future.isDone())
            {
                workCount++;

                try
                {
                    future.get();
                    context.take(TRANSITION_DEFAULT);
                }
                catch (final Exception e)
                {
                    logger.warn("Failed to open log stream controller", e);
                    context.decreaseRetries();
                    context.take(TRANSITION_FAILED);
                }
                finally
                {
                    context.setFuture(null);
                }
            }

            return workCount;
        }

        @Override
        public boolean isInterruptable()
        {
            return false;
        }

    }

    static class AppendInitialEventState implements State<Context>
    {

        @Override
        public int doWork(final Context context) throws Exception
        {
            final long position = context.tryWriteInitialEvent();

            if (position >= 0)
            {
                context.setPosition(position);
                context.take(TRANSITION_DEFAULT);
            }
            else
            {
                context.getRaft().getLogger().debug("Failed to append initial event");
                context.take(TRANSITION_FAILED);
            }

            return 1;
        }

        @Override
        public boolean isInterruptable()
        {
            return false;
        }
    }

    static class AwaitInitialEventAppendedState implements State<Context>
    {

        @Override
        public int doWork(final Context context) throws Exception
        {
            int workCount = 0;

            final Raft raft = context.getRaft();
            final Logger logger = raft.getLogger();

            if (context.isAppended())
            {
                logger.debug("Initial event for term {} was appended in position {}", raft.getTerm(), context.getPosition());

                workCount++;
                context.take(TRANSITION_DEFAULT);
            }
            else
            {
                logger.debug("Failed to append initial event in position {}", context.getPosition());

                workCount++;
                context.resetPosition();
                context.take(TRANSITION_FAILED);
            }

            return workCount;
        }

    }

    static class AwaitInitialEventCommittedState implements State<Context>
    {

        @Override
        public int doWork(final Context context) throws Exception
        {
            int workCount = 0;

            if (context.isCommitted())
            {
                final Raft raft = context.getRaft();
                raft.getLogger().debug("Initial event for term {} was committed on position {}", raft.getTerm(), context.getPosition());

                workCount++;
                context.take(TRANSITION_DEFAULT);
            }

            return workCount;
        }

    }

    static class CloseLogControllerState implements State<Context>
    {

        @Override
        public int doWork(final Context context) throws Exception
        {
            final LogStream logStream = context.getRaft().getLogStream();

            context.setFuture(logStream.closeLogStreamController());
            context.take(TRANSITION_DEFAULT);

            return 1;
        }

        @Override
        public boolean isInterruptable()
        {
            return false;
        }

    }

    static class AwaitCloseLogControllerState implements State<Context>
    {

        @Override
        public int doWork(final Context context) throws Exception
        {
            int workCount = 0;

            final Logger logger = context.getRaft().getLogger();
            final ActorFuture<Void> future = context.getFuture();

            if (future.isDone())
            {
                workCount++;

                try
                {
                    future.get();
                    context.take(TRANSITION_DEFAULT);
                }
                catch (final Exception e)
                {
                    logger.warn("Failed to close log stream controller", e);
                    context.take(TRANSITION_DEFAULT);
                }
                finally
                {
                    context.reset();
                }
            }

            return workCount;
        }

        @Override
        public boolean isInterruptable()
        {
            return false;
        }

    }

    static class FailedToOpenLogControllerState implements State<Context>
    {

        @Override
        public int doWork(final Context context) throws Exception
        {
            // step down as we failed to open the log controller to write events
            context.getRaft().becomeFollower();
            context.reset();
            context.take(TRANSITION_DEFAULT);
            return 1;
        }

        @Override
        public boolean isInterruptable()
        {
            return false;
        }

    }

    static class Context extends SimpleStateMachineContext
    {

        private final Raft raft;

        private final InitialEvent initialEvent = new InitialEvent();

        private ActorFuture<Void> future;
        private long position;
        private long retries;

        Context(final StateMachine<Context> stateMachine, final Raft raft)
        {
            super(stateMachine);
            this.raft = raft;

            reset();
        }

        @Override
        public void reset()
        {
            initialEvent.reset();

            future = null;
            position = -1;
            retries = 10;
        }

        public Raft getRaft()
        {
            return raft;
        }

        public void setFuture(final ActorFuture<Void> future)
        {
            this.future = future;
        }

        public ActorFuture<Void> getFuture()
        {
            return future;
        }

        public long tryWriteInitialEvent()
        {
            return initialEvent.tryWrite(raft);
        }

        public void setPosition(final long position)
        {
            this.position = position;
        }

        public boolean isAppended()
        {
            return position >= 0 && position < raft.getLogStream().getCurrentAppenderPosition();
        }

        public boolean isCommitted()
        {
            return position >= 0 && position <= raft.getLogStream().getCommitPosition();
        }

        public void resetPosition()
        {
            position = -1;
        }

        public long getPosition()
        {
            return position;
        }

        public boolean hasRetriesLeft()
        {
            return retries > 0;
        }

        public void decreaseRetries()
        {
            retries--;
        }
    }

}
