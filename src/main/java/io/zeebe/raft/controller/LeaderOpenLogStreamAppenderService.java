package io.zeebe.raft.controller;

import io.zeebe.logstreams.log.LogStream;
import io.zeebe.servicecontainer.*;

public class LeaderOpenLogStreamAppenderService implements Service<Void>
{
    private final LogStream logStream;

    public LeaderOpenLogStreamAppenderService(LogStream logStream)
    {
        this.logStream = logStream;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        startContext.async(logStream.openAppender());
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(logStream.closeAppender());
    }

    @Override
    public Void get()
    {
        return null;
    }

}
