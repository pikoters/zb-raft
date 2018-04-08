package io.zeebe.raft.state;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import io.zeebe.raft.Raft;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.ScheduledTimer;

public abstract class ElectionState extends AbstractRaftState
{
    private ScheduledTimer scheduledElection;
    private boolean skipNextElection = false;

    public ElectionState(Raft raft, ActorControl raftActor)
    {
        super(raft, raftActor);
    }

    @Override
    protected void onEnterState()
    {
        super.onEnterState();
        scheduledElection = raftActor.runDelayed(nextElectionTimeout(), this::electionTimeoutCallback);
    }

    @Override
    protected void onLeaveState()
    {
        scheduledElection.cancel();
        super.onLeaveState();
    }

    @Override
    protected void skipNextElection()
    {
        skipNextElection = true;
    }

    public Duration nextElectionTimeout()
    {
        final int electionIntervalMs = raft.getConfiguration().getElectionIntervalMs();
        return Duration.ofMillis(electionIntervalMs + (Math.abs(ThreadLocalRandom.current().nextInt()) % electionIntervalMs));
    }

    private void electionTimeoutCallback()
    {
        scheduledElection = raftActor.runDelayed(nextElectionTimeout(), this::electionTimeoutCallback);

        if (!skipNextElection)
        {
            onElectionTimeout();
        }

        skipNextElection = false;
    }

    protected abstract void onElectionTimeout();
}
