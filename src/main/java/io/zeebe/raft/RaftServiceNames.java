package io.zeebe.raft;

import io.zeebe.raft.state.RaftState;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.transport.SocketAddress;

public class RaftServiceNames
{
    public static ServiceName<Void> joinServiceName(String raftName)
    {
        return ServiceName.newServiceName(String.format("raft.%s.joinService", raftName), Void.class);
    }

    public static ServiceName<Void> leaderInstallServiceName(String raftName, int term)
    {
        return ServiceName.newServiceName(String.format("raft.leader.%s.%d.install", raftName, term), Void.class);
    }

    public static ServiceName<Void> leaderOpenLogStreamServiceName(String raftName, int term)
    {
        return ServiceName.newServiceName(String.format("raft.leader.%s.%d.openLogStream", raftName, term), Void.class);
    }

    public static ServiceName<Void> leaderInitialEventAppendedServiceName(String raftName, int term)
    {
        return ServiceName.newServiceName(String.format("raft.leader.%s.%d.initialEventCommitted", raftName, term), Void.class);
    }

    public static ServiceName<Void> replicateLogConrollerServiceName(String raftName, int term, SocketAddress follower)
    {
        return ServiceName.newServiceName(String.format("raft.leader.%s.%d.replicate.%s", raftName, term, follower), Void.class);
    }

    public static ServiceName<Void> appendRaftEventControllerServiceName(String raftName, int term, SocketAddress member)
    {
        return ServiceName.newServiceName(String.format("raft.leader.%s.%d.appendRaftEvent.%s", raftName, term, member), Void.class);
    }

    public static ServiceName<RaftState> leaderServiceName(String raftName, int term)
    {
        return ServiceName.newServiceName(String.format("raft.leader.%s.%d", raftName, term), RaftState.class);
    }

    public static ServiceName<RaftState> followerServiceName(String raftName, int term)
    {
        return ServiceName.newServiceName(String.format("raft.follower.%s.%d", raftName, term), RaftState.class);
    }

    public static ServiceName<RaftState> candidateServiceName(String raftName, int term)
    {
        return ServiceName.newServiceName(String.format("raft.candidate.%s.%d", raftName, term), RaftState.class);
    }
}
