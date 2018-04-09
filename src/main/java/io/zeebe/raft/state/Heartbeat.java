package io.zeebe.raft.state;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import io.zeebe.util.sched.clock.ActorClock;

public class Heartbeat
{
    private final int electionInterval;
    private long lastHeartbeat = 0;

    public Heartbeat(int electionInterval)
    {
        this.electionInterval = electionInterval;
    }

    public void updateLastHeartbeat()
    {
        lastHeartbeat = ActorClock.currentTimeMillis();
    }

    public boolean shouldElect()
    {
        return ActorClock.currentTimeMillis() >= (lastHeartbeat + electionInterval);
    }

    public Duration nextElectionTimeout()
    {
        return Duration.ofMillis(electionInterval + (Math.abs(ThreadLocalRandom.current().nextInt()) % electionInterval));
    }
}
