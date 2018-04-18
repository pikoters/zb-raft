/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.raft.state;

import io.zeebe.raft.Raft;
import io.zeebe.raft.controller.ConsensusRequestController;
import io.zeebe.raft.controller.VoteRequestHandler;
import io.zeebe.raft.protocol.AppendRequest;
import io.zeebe.util.sched.ActorControl;

public class CandidateState extends ElectionState
{
    protected final ConsensusRequestController voteController;

    public CandidateState(Raft raft, ActorControl raftActor, int term)
    {
        super(raft, raftActor, term);
        voteController = new ConsensusRequestController(raft, raftActor, new VoteRequestHandler()
        {
            @Override
            public void consensusGranted(Raft raft)
            {
                raft.becomeLeader(term);
            }

            @Override
            public void consensusFailed(Raft raft)
            {
                raft.becomeFollower(term);
            }
        });
    }

    @Override
    public RaftState getState()
    {
        return RaftState.CANDIDATE;
    }

    @Override
    protected void onLeaveState()
    {
        voteController.close();
        super.onLeaveState();
    }

    @Override
    protected void onElectionTimeout()
    {
        voteController.sendRequest();
    }

    @Override
    public void appendRequest(final AppendRequest appendRequest)
    {
        if (raft.isTermCurrent(appendRequest))
        {
            heartbeat.updateLastHeartbeat();
            // received append request from new leader
            raft.becomeFollower(appendRequest.getTerm());
        }
    }
}
