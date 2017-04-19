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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ElasticsearchTable
{
    private final String name;

    private List<ElasticsearchColumn> columns;
    private final List<ElasticsearchColumnHandle> sources;
    private final List<ColumnMetadata> columnMetadatas;

    @JsonCreator
    public ElasticsearchTable(
            @JsonProperty("name") String name,
            @JsonProperty("sources") List<ElasticsearchColumnHandle> sources,
            @JsonProperty("columnMetadatas") List<ColumnMetadata> columnMetadatas)
    {
        this.name = requireNonNull(name, "name is null").toLowerCase(ENGLISH);
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        this.columnMetadatas = ImmutableList.copyOf(requireNonNull(columnMetadatas, "columnMetadatas is null"));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<ElasticsearchColumn> getColumns()
    {
        return columns;
    }

    public void setColumns(List<ElasticsearchColumn> columns)
    {
        this.columns = columns;
    }

    @JsonProperty
    public List<ElasticsearchColumnHandle> getSources()
    {
        return sources;
    }

    @JsonProperty
    public List<ColumnMetadata> getColumnMetadatas()
    {
        return columnMetadatas;
    }}
