package io.zeebe.raft.controller;

import io.zeebe.raft.Raft;
import io.zeebe.raft.protocol.PollRequest;
import io.zeebe.raft.protocol.PollResponse;
import io.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;

/**
 *
 */
public class PollRequestResponseAssistant implements RequestResponseAssistant
{
    private final PollRequest pollRequest = new PollRequest();
    private final PollResponse pollResponse = new PollResponse();

    @Override
    public BufferWriter createRequest(Raft raft, long lastEventPosition, int lastTerm)
    {
        return pollRequest.reset()
                          .setRaft(raft)
                          .setLastEventPosition(lastEventPosition)
                          .setLastEventTerm(lastTerm);
    }

    @Override
    public boolean isResponseGranted(Raft raft, DirectBuffer responseBuffer)
    {
        pollResponse.wrap(responseBuffer, 0, responseBuffer.capacity());

        // only register response from the current term
        return !raft.mayStepDown(pollResponse) &&
            raft.isTermCurrent(pollResponse) &&
            pollResponse.isGranted();
    }

    @Override
    public void reset()
    {
        pollRequest.reset();
        pollResponse.reset();
    }

    @Override
    public void requestSuccessful(Raft raft)
    {
        raft.becomeCandidate();
    }

    @Override
    public void requestFailed(Raft raft)
    {
    }
}
