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
import io.zeebe.raft.Loggers;
import io.zeebe.raft.Raft;
import io.zeebe.raft.event.InitialEvent;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import org.slf4j.Logger;

import java.time.Duration;

public class OpenLogStreamController
{
    private static final Logger LOG = Loggers.RAFT_LOGGER;
    public static final Duration COMMIT_TIMEOUT = Duration.ofMinutes(15);

    private final ActorControl actor;
    private final Raft raft;
    private final InitialEvent initialEvent = new InitialEvent();
    private final ActorCondition actorCondition;

    private long position;
    private long retries = 10;
    private boolean isCommited;

    public OpenLogStreamController(final Raft raft, ActorControl actorControl)
    {
        this.raft = raft;
        this.actor = actorControl;

        this.actorCondition = actor.onCondition("raft-event-commited", this::commited);
    }

    public long getPosition()
    {
        return position;
    }

    public void open()
    {
        if (hasRetriesLeft())
        {
            final LogStream logStream = raft.getLogStream();

            LOG.debug("Open log stream ctrl");
            final ActorFuture<Void> future = logStream.openLogStreamController();

            actor.runOnCompletion(future, ((aVoid, throwable) ->
            {
                if (throwable == null)
                {
                    retries = 10;
                    actor.submit(this::appendInitialEvent);
                }
                else
                {
                    LOG.warn("Failed to sendRequest log stream controller.r", throwable);
                    decreaseRetries();
                    actor.run(this::open);
                }
            }));
        }
        else
        {
            raft.becomeFollower();
        }
    }

    private void appendInitialEvent()
    {
        LOG.debug("try to append init event");
        final long position = initialEvent.tryWrite(raft);
        if (position >= 0)
        {
            this.position = position;

            LOG.debug("register on condition");
            raft.getLogStream().registerOnCommitPositionUpdatedCondition(actorCondition);
            actor.runDelayed(COMMIT_TIMEOUT, () ->
            {
                if (!isCommited)
                {
                    actor.submit(() -> appendInitialEvent());
                }
            });

        }
        else
        {
            LOG.debug("Failed to append initial event");
            actor.submit(this::appendInitialEvent);
        }
    }

    private void commited()
    {
        LOG.debug("On commited");
        if (isPositionCommited())
        {
            LOG.debug("Initial event for term {} was committed on position {}", raft.getTerm(), position);

            isCommited = true;
            raft.getLogStream().removeOnCommitPositionUpdatedCondition(actorCondition);
        }
    }

    public void close()
    {
        final LogStream logStream = raft.getLogStream();

        raft.getLogStream().removeOnCommitPositionUpdatedCondition(actorCondition);
        final ActorFuture<Void> future = logStream.closeLogStreamController();

        actor.runOnCompletion(future, ((aVoid, throwable) ->
        {
            if (throwable != null)
            {
                LOG.warn("Failed to close log stream controller", throwable);
            }
        }));
        retries = 10;
    }

    public boolean isPositionCommited()
    {
        return position >= 0 && position <= raft.getLogStream().getCommitPosition();
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
