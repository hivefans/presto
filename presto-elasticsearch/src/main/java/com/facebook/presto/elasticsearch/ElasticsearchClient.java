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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.inject.Inject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ElasticsearchClient
{
    public final Client client;

    @Inject
    public ElasticsearchClient(ElasticsearchConfig config)
            throws IOException
    {
        requireNonNull(config, "config is null");
        this.client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(config.getHostname()), config.getPort()));
    }

    public ElasticsearchClient(Client client)
    {
        requireNonNull(client, "client is null");
        this.client = client;
    }

    public List<String> getSchemaNames()
    {
        return Arrays.asList(client.admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getConcreteAllIndices());
    }

    public List<String> getAllTables(String databaseName)
    {
        // TODO: this logic is not efficient
        List<String> tables = new ArrayList<>();
        ImmutableOpenMap<String, IndexMetaData> res = client.admin().cluster().prepareState().get().getState().getMetaData().getIndices();
        for (String indice : res.keys().toArray(String.class)) {
            if (!indice.equals(databaseName)) {
                continue;
            }

            for (ObjectObjectCursor<String, MappingMetaData> mapping : res.get(indice).getMappings()) {
                tables.add(mapping.key);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    public ElasticsearchTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        // TODO: need to be modified
        ImmutableOpenMap<String, IndexMetaData> res = client.admin().cluster().prepareState().get().getState().getMetaData().getIndices();
        List<ElasticsearchColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMetadata> columnMetadatas = new ArrayList<>();
        for (ObjectObjectCursor<String, MappingMetaData> metadata : res.get(schema).getMappings()) {
            MappingMetaData meta = metadata.value;

            try {
                Map<String, Object> map = (Map<String, Object>) meta.sourceAsMap().get("properties");
                Set<String> keys = map.keySet();
                for (String key : keys) {
                    String columnName = key;
                    String jsonType = ((Map<String, Object>) map.get(key)).get("type").toString();
                    Type prestoType = toPrestoType(jsonType);

                    columnHandles.add(new ElasticsearchColumnHandle(columnName, prestoType, jsonType, 0));
                    columnMetadatas.add(new ColumnMetadata(columnName, prestoType));
                }
            }
            catch (IOException e) {
            }
        }
        return new ElasticsearchTable(tableName, columnHandles, columnMetadatas);
    }

    Type toPrestoType(String type)
    {
        Type prestoType;

        switch (type) {
            case "double":
            case "float":
                prestoType = DOUBLE;
                break;
            case "integer":
                prestoType = INTEGER;
                break;
            case "long":
                prestoType = BIGINT;
                break;
            case "string":
                prestoType = VARCHAR;
                break;
            case "boolean":
                prestoType = BOOLEAN;
                break;
            case "binary":
                prestoType = VARBINARY;
                break;
            case "nested":
                prestoType = VARCHAR;
                break;
            default:
                prestoType = VARCHAR;
                break;
        }

        return prestoType;
    }
}
