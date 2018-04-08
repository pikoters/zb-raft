package io.zeebe.raft.controller;

import java.time.Duration;

import io.zeebe.raft.Loggers;
import io.zeebe.raft.Raft;
import io.zeebe.raft.protocol.JoinRequest;
import io.zeebe.raft.protocol.JoinResponse;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

public class JoinRaftController implements Service<Void>
{
    private static final Logger LOG = Loggers.RAFT_LOGGER;

    public static final Duration DEFAULT_JOIN_TIMEOUT = Duration.ofMillis(500);
    public static final Duration DEFAULT_JOIN_RETRY = Duration.ofMillis(200);

    private final ActorControl actor;
    private final JoinRequest joinRequest = new JoinRequest();
    private final JoinResponse joinResponse = new JoinResponse();
    private final Raft raft;

    // will not be reset to continue to select new members on every retry
    private int currentMember;

    private final CompletableActorFuture<Void> joinFuture = new CompletableActorFuture<>();

    public JoinRaftController(final Raft raft, ActorControl actorControl)
    {
        this.raft = raft;
        this.actor = actorControl;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        actor.call(this::join);
        startContext.async(joinFuture);
    }

    public void join()
    {
        final RemoteAddress nextMember = getNextMember();

        if (nextMember != null)
        {
            joinRequest.reset().setRaft(raft);

            LOG.debug("Send join request to {}", nextMember);
            final ActorFuture<ClientResponse> responseFuture = raft.sendRequest(nextMember, joinRequest, DEFAULT_JOIN_TIMEOUT);

            actor.runOnCompletion(responseFuture, ((response, throwable) ->
            {
                if (throwable == null)
                {
                    try
                    {
                        final DirectBuffer responseBuffer = response.getResponseBuffer();
                        joinResponse.wrap(responseBuffer, 0, responseBuffer.capacity());
                    }
                    finally
                    {
                        response.close();
                    }

                    if (!raft.mayStepDown(joinResponse) && raft.isTermCurrent(joinResponse))
                    {
                        // update members to maybe discover leader
                        raft.addMembers(joinResponse.getMembers());

                        if (joinResponse.isSucceeded())
                        {
                            LOG.debug("Join request was accepted in term {}", joinResponse.getTerm());

                            // as this will not trigger a state change in raft we have to notify listeners
                            // that this raft is now in a visible state
                            raft.notifyRaftStateListeners();
                            joinFuture.complete(null);
                        }
                        else
                        {
                            LOG.debug("Join was not accepted!");
                            actor.runDelayed(DEFAULT_JOIN_RETRY, this::join);
                        }
                    }
                    else
                    {
                        LOG.debug("Join response with different term.");
                        // received response from different term
                        actor.runDelayed(DEFAULT_JOIN_RETRY, this::join);
                    }
                }
                else
                {
                    LOG.debug("Failed to send join request to {}", nextMember);
                    actor.runDelayed(DEFAULT_JOIN_RETRY, this::join);
                }
            }));
        }
        else
        {
            LOG.debug("Joined single node cluster.");
            joinFuture.complete(null);
        }
    }

    private RemoteAddress getNextMember()
    {
        final int memberSize = raft.getMemberSize();
        if (memberSize > 0)
        {
            final int nextMember = currentMember % memberSize;
            currentMember++;

            return raft.getMember(nextMember).getRemoteAddress();
        }
        else
        {
            return null;
        }
    }

    @Override
    public Void get()
    {
        return null;
    }

    public boolean isJoined()
    {
        return isJoined();
    }

}
