package io.zeebe.raft.controller;

import io.zeebe.raft.Raft;
import io.zeebe.raft.protocol.VoteRequest;
import io.zeebe.raft.protocol.VoteResponse;
import io.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;

/**
 *
 */
public class VoteRequestResponseAssistant implements RequestResponseAssistant
{
    private final VoteRequest voteRequest = new VoteRequest();
    private final VoteResponse voteResponse = new VoteResponse();

    @Override
    public BufferWriter createRequest(Raft raft, long lastEventPosition, int lastTerm)
    {
        return voteRequest.reset()
                          .setRaft(raft)
                          .setLastEventPosition(lastEventPosition)
                          .setLastEventTerm(lastTerm);
    }

    @Override
    public boolean isResponseGranted(Raft raft, DirectBuffer responseBuffer)
    {
        voteResponse.wrap(responseBuffer, 0, responseBuffer.capacity());

        return voteResponse.isGranted();
    }

    @Override
    public void reset()
    {
        voteRequest.reset();
        voteResponse.reset();
    }

    @Override
    public void requestSuccessful(Raft raft)
    {
        raft.becomeLeader();
    }

    @Override
    public void requestFailed(Raft raft)
    {
        raft.becomeFollower();
    }
}
