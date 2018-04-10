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
            scheduledElection = raftActor.runDelayed(heartbeat.nextElectionTimeout(), this::electionTimeoutCallback);
        }
    }

    @Override
    protected void onLeaveState()
    {
        scheduledElection.cancel();
        super.onLeaveState();
    }

    private void electionTimeoutCallback()
    {
        scheduledElection = raftActor.runDelayed(heartbeat.nextElectionTimeout(), this::electionTimeoutCallback);

        if (heartbeat.shouldElect())
        {
            onElectionTimeout();
        }
    }

    protected abstract void onElectionTimeout();
}
