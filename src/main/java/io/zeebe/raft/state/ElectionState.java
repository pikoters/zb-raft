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

import io.zeebe.raft.Raft;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.ScheduledTimer;

public abstract class ElectionState extends AbstractRaftState
{
    private ScheduledTimer scheduledElection;

    public ElectionState(Raft raft, ActorControl raftActor)
    {
        super(raft, raftActor);
    }

    @Override
    protected void onEnterState()
    {
        super.onEnterState();

        if (raftMembers.getMemberSize() == 0)
        {
            raftActor.submit(this::electionTimeoutCallback);
        }
        else
        {
            scheduleElectionTimer();
        }
    }

    @Override
    protected void onLeaveState()
    {
        if (scheduledElection != null)
        {
            scheduledElection.cancel();
        }

        super.onLeaveState();
    }

    protected void scheduleElectionTimer()
    {
        scheduledElection = raftActor.runDelayed(heartbeat.nextElectionTimeout(), this::electionTimeoutCallback);
    }

    private void electionTimeoutCallback()
    {
        if (heartbeat.shouldElect())
        {
            onElectionTimeout();
        }
        else
        {
            scheduleElectionTimer();
        }
    }

    protected abstract void onElectionTimeout();
}
