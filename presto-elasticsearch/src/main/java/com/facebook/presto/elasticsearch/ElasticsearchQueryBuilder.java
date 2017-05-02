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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Strings;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.List;

public class ElasticsearchQueryBuilder
{
    private static final int SCROLL_TIME = 60000;
    private static final int SCROLL_SIZE = 5000;

    final Client client;
    final TupleDomain<ColumnHandle> tupleDomain;
    final List<ElasticsearchColumnHandle> columns;
    final boolean isToAddFields;

    private final String index;
    private final String type;

    public ElasticsearchQueryBuilder(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchSplit split, ElasticsearchClient elasticsearchClient, boolean isToAddFields)
    {
        this.index = split.getSchemaName();
        this.type = split.getTableName();
        this.client = elasticsearchClient.client;
        this.tupleDomain = split.getTupleDomain();
        this.columns = columnHandles;
        this.isToAddFields = isToAddFields;
    }

    public SearchRequestBuilder buildScrollSearchRequest()
    {
        SearchRequestBuilder searchRequestBuilder = client
                    .prepareSearch(Strings.isNullOrEmpty(index) ? "_all" : index)
                    .setTypes(type)
                    .setSearchType(SearchType.DEFAULT)
                    .setScroll(new TimeValue(SCROLL_TIME))
                    .setQuery(getSearchQuery())
                    .setSize(SCROLL_SIZE); // per shard

        // elasticsearch doesn't support adding fields when there is a nested type
        if (isToAddFields) {
            for (ElasticsearchColumnHandle columnHandle : columns) {
                searchRequestBuilder.addStoredField(columnHandle.getColumnName());
            }
        }

        return searchRequestBuilder;
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId)
    {
        return client
            .prepareSearchScroll(scrollId)
            .setScroll(new TimeValue(SCROLL_TIME));
    }

    // todo: Develop filtering in Elasticsearch layer. Refer QueryBuilder.java
    private BoolQueryBuilder getSearchQuery()
    {
        BoolQueryBuilder boolFilterBuilder = new BoolQueryBuilder();

        for (ElasticsearchColumnHandle column : columns) {
            tupleDomain
                    .getDomains()
                    .ifPresent((e) -> {
                        Domain domain = e.get(column);
                        if (domain != null) {
//                            boolFilterBuilder.must(QueryBuilders.termQuery(column.getColumnName(), "kimchy"));
                        }
                    });
        }
        return boolFilterBuilder;
    }

    private boolean isAcceptedType(Type type)
    {
        if (type.equals(BigintType.BIGINT)) {
            return true;
        }
        else if (type.equals(IntegerType.INTEGER)) {
            return true;
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return true;
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            return true;
        }
        else if (type.equals(BooleanType.BOOLEAN)) {
            return true;
        }
        else {
           throw new UnsupportedOperationException("Query Builder can't handle type: " + type);
        }
    }
}
