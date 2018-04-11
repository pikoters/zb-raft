package io.zeebe.raft;

public class RaftMath
{
    public static int getRequiredQuorum(int membersize)
    {
        return Math.floorDiv(membersize, 2) + 1;
    }
}
