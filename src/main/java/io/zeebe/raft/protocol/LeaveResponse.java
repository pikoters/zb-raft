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
package io.zeebe.raft.protocol;

import io.zeebe.raft.BooleanType;
import io.zeebe.raft.LeaveResponseDecoder;
import io.zeebe.raft.LeaveResponseEncoder;
import io.zeebe.raft.Raft;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import static io.zeebe.raft.JoinResponseEncoder.MembersEncoder.*;
import static io.zeebe.raft.JoinResponseEncoder.termNullValue;

public class LeaveResponse extends AbstractRaftMessage implements HasTerm
{
    protected final LeaveResponseDecoder bodyDecoder = new LeaveResponseDecoder();
    protected final LeaveResponseEncoder bodyEncoder = new LeaveResponseEncoder();

    // read + write
    protected int term;
    protected boolean succeeded;

    public LeaveResponse()
    {
        reset();
    }

    public LeaveResponse reset()
    {
        term = termNullValue();
        succeeded = false;

        return this;
    }

    @Override
    protected int getVersion()
    {
        return bodyDecoder.sbeSchemaVersion();
    }

    @Override
    protected int getSchemaId()
    {
        return bodyDecoder.sbeSchemaId();
    }

    @Override
    protected int getTemplateId()
    {
        return bodyDecoder.sbeTemplateId();
    }

    @Override
    public int getTerm()
    {
        return term;
    }

    public boolean isSucceeded()
    {
        return succeeded;
    }

    public LeaveResponse setSucceeded(final boolean succeeded)
    {
        this.succeeded = succeeded;
        return this;
    }

    public LeaveResponse setRaft(final Raft raft)
    {
        term = raft.getTerm();
         return this;
    }

    @Override
    public int getLength()
    {
       int length = headerEncoder.encodedLength() +
            bodyEncoder.sbeBlockLength() +
            sbeHeaderSize() + (sbeBlockLength() + hostHeaderLength());

        return length;
    }

    @Override
    public void wrap(final DirectBuffer buffer, int offset, final int length)
    {
        reset();

        final int frameEnd = offset + length;

        headerDecoder.wrap(buffer, offset);
        offset += headerDecoder.encodedLength();

        bodyDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

        term = bodyDecoder.term();
        succeeded = bodyDecoder.succeeded() == BooleanType.TRUE;

        assert bodyDecoder.limit() == frameEnd : "Decoder read only to position " + bodyDecoder.limit() + " but expected " + frameEnd + " as final position";
    }

    @Override
    public void write(final MutableDirectBuffer buffer, int offset)
    {
        headerEncoder.wrap(buffer, offset)
                     .blockLength(bodyEncoder.sbeBlockLength())
                     .templateId(bodyEncoder.sbeTemplateId())
                     .schemaId(bodyEncoder.sbeSchemaId())
                     .version(bodyEncoder.sbeSchemaVersion());

        offset += headerEncoder.encodedLength();

        bodyEncoder.wrap(buffer, offset)
                   .term(term)
                   .succeeded(succeeded ? BooleanType.TRUE : BooleanType.FALSE);
    }
}
