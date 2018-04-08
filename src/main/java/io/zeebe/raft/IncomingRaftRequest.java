package io.zeebe.raft;

import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerOutput;
import org.agrona.MutableDirectBuffer;

public class IncomingRaftRequest
{
    private ServerOutput output;
    private RemoteAddress remoteAddress;
    private long requestId;
    private MutableDirectBuffer requestData;

    public IncomingRaftRequest(ServerOutput output,
        RemoteAddress remoteAddress,
        long requestId,
        MutableDirectBuffer requestData)
    {
        this.output = output;
        this.remoteAddress = remoteAddress;
        this.requestId = requestId;
        this.requestData = requestData;
    }

    public ServerOutput getOutput()
    {
        return output;
    }

    public RemoteAddress getRemoteAddress()
    {
        return remoteAddress;
    }

    public long getRequestId()
    {
        return requestId;
    }

    public MutableDirectBuffer getRequestData()
    {
        return requestData;
    }
}
