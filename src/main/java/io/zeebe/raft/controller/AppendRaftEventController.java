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
import io.zeebe.raft.event.RaftEvent;
import io.zeebe.raft.protocol.ConfigurationResponse;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerOutput;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorControl;
import org.slf4j.Logger;

import java.time.Duration;

public class AppendRaftEventController implements Service<Void>
{
    private static final Logger LOG = Loggers.RAFT_LOGGER;
    public static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(15);

    private final Raft raft;
    private final ActorControl actor;

    private final RaftEvent raftEvent = new RaftEvent();
    private final ConfigurationResponse configurationResponse = new ConfigurationResponse();
    private final ActorCondition actorCondition;

    private long position;

    // response state
    private final ServerOutput serverOutput;
    private final RemoteAddress remoteAddress;
    private final long requestId;

    private boolean isCommited;

    public AppendRaftEventController(final Raft raft,
        ActorControl actorControl,
        final ServerOutput serverOutput,
        final RemoteAddress remoteAddress,
        final long requestId)
    {
        this.raft = raft;
        this.actor = actorControl;
        this.serverOutput = serverOutput;
        this.remoteAddress = remoteAddress;
        this.requestId = requestId;

        this.actorCondition = actor.onCondition("raft-event-commited", this::commited);
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        actor.call(this::appendEvent);
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        actorCondition.cancel();
    }

    public void appendEvent()
    {
        final long position = raftEvent.tryWrite(raft);
        if (position >= 0)
        {
            this.position = position;
            raft.getLogStream().registerOnCommitPositionUpdatedCondition(actorCondition);
            actor.runDelayed(COMMIT_TIMEOUT, () ->
            {
                if (!isCommited)
                {
                    actor.submit(this::appendEvent);
                }
            });
        }
        else
        {
            LOG.debug("Failed to append raft event");
            actor.submit(this::appendEvent);
        }
    }

    private void commited()
    {
        if (isCommitted())
        {
            LOG.debug("Raft event for term {} was committed on position {}", raft.getTerm(), position);

            // send response
            acceptConfigurationRequest();

            isCommited = true;
            actorCondition.cancel();
        }
    }

    public boolean isCommitted()
    {
        return position >= 0 && position <= raft.getLogStream().getCommitPosition();
    }

    private void acceptConfigurationRequest()
    {
        configurationResponse
            .reset()
            .setSucceeded(true)
            .setRaft(raft);

        raft.sendResponse(serverOutput, remoteAddress, requestId, configurationResponse);
    }
    public long getPosition()
    {
        return position;
    }

    @Override
    public Void get()
    {
        return null;
    }
}
