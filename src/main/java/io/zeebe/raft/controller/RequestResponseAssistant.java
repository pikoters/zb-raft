package io.zeebe.raft.controller;

import io.zeebe.raft.Raft;
import io.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;

/**
 *
 */
public interface RequestResponseAssistant
{
    /**
     * Creates specific request.
     *
     * @param raft
     * @param lastEventPosition
     * @param lastTerm
     * @return
     */
    BufferWriter createRequest(Raft raft, long lastEventPosition, int lastTerm);

    /**
     * Checks if the response was granted, quorum was reached.
     *
     * @param raft
     * @param responseBuffer
     * @return
     */
    boolean isResponseGranted(Raft raft, DirectBuffer responseBuffer);

    /**
     * Callback if the request was successful.
     *
     * @param raft
     */
    void requestSuccessful(Raft raft);

    /**
     * Callback if the request failed.
     *
     * @param raft
     */
    void requestFailed(Raft raft);

    /**
     * Reset the assistant.
     */
    void reset();
}
