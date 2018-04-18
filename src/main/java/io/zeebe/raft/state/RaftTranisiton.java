package io.zeebe.raft.state;

public enum RaftTranisiton
{
    NEW_TO_FOLLOWER,

    FOLLOWER_TO_CANDIDATE,

    CANDIDATE_TO_LEADER,
    CANDIDATE_TO_FOLLOWER,

    LEADER_TO_FOLLOWER;
}
