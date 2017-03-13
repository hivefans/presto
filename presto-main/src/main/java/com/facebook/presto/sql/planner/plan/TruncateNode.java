/*
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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.TableWriterNode.TruncateHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class TruncateNode
        extends PlanNode
{
    private final PlanNode source;
    private final TruncateHandle target;
    private final List<Symbol> outputs;

    @JsonCreator
    public TruncateNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") TruncateHandle target,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.outputs = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public TruncateHandle getTarget()
    {
        return target;
    }

    @JsonProperty("outputs")
    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTruncate(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TruncateNode(getId(), Iterables.getOnlyElement(newChildren), target, outputs);
    }
}
