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
package io.zeebe.raft.event;

import java.util.List;

import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.msgpack.value.ValueArray;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftMember;

public class RaftEvent
{
    public final LogStreamWriter logStreamWriter = new LogStreamWriterImpl();
    public final BrokerEventMetadata metadata = new BrokerEventMetadata();
    public final RaftConfigurationEvent configuration = new RaftConfigurationEvent();

    public RaftEvent reset()
    {
        logStreamWriter.reset();
        metadata.reset();
        configuration.reset();

        return this;
    }

    public long tryWrite(final Raft raft)
    {
        logStreamWriter.wrap(raft.getLogStream());

        metadata.reset().eventType(EventType.RAFT_EVENT);

        configuration.reset();

        final ValueArray<RaftConfigurationEventMember> configurationMembers = configuration.members();

        // add self also to configuration
        configurationMembers.add().setSocketAddress(raft.getSocketAddress());

        final List<RaftMember> memberList = raft.getRaftMembers().getMemberList();
        for (final RaftMember member : memberList)
        {
            configurationMembers.add().setSocketAddress(member.getRemoteAddress().getAddress());
        }

        return logStreamWriter
            .positionAsKey()
            .metadataWriter(metadata)
            .valueWriter(configuration)
            .tryWrite();
    }
}
