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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.inject.Inject;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ElasticsearchClient
{
    private final Logger log = Logger.get(ElasticsearchClient.class);

    private Supplier<Map<String, Map<String, ElasticsearchTable>>> schemas;
    private ElasticsearchConfig config;
    private JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec;
    private ImmutableMap<String, Client> internalClients;
    public final Client client;

    @Inject
    public ElasticsearchClient(ElasticsearchConfig config)
            throws IOException
    {
        requireNonNull(config, "config is null");

        this.config = config;
        this.client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(config.getHostname()), config.getPort()));
    }

    public ImmutableMap<String, Client> getInternalClients()
    {
        return internalClients;
    }

    public List<String> getSchemaNames()
    {
        return Arrays.asList(client.admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getConcreteAllIndices());
    }

    public Optional<List<String>> getAllTables(String databaseName)
    {
        List<String> tables = new ArrayList<>();
        ImmutableOpenMap<String, IndexMetaData> res = client.admin().cluster().prepareState().get().getState().getMetaData().getIndices();
        for (String indice : res.keys().toArray(String.class)) {
            tables.add(indice);
        }
        return Optional.of(ImmutableList.copyOf(tables));
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

            List<String> fieldList = new ArrayList<String>();

            try {
                Map<String, Object> map = (Map<String, Object>) meta.sourceAsMap().get("properties");
                Set<String> keys = map.keySet();
                for (String key : keys) {
                    String columnName = key;
                    Type columnType = toPrestoType(((Map<String, Object>) map.get(key)).get("type").toString());

                    columnHandles.add(new ElasticsearchColumnHandle("", columnName, columnType, "", "", 0));
                    columnMetadatas.add(new ColumnMetadata(columnName, columnType));
                }
            }
            catch (IOException e) {
            }
        }
        return new ElasticsearchTable(tableName, columnHandles, columnMetadatas);
    }

    ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getMappings(ElasticsearchTableSource src)
            throws ExecutionException, InterruptedException
    {
        int port = src.getPort();
        String hostAddress = src.getHostAddress();
        String clusterName = src.getClusterName();
        String index = src.getIndex();
        String type = src.getType();

        Client client = internalClients.get(clusterName);
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().types(type);

        // an index is optional - if no index is configured for the table, it will retrieve all indices for the doc type
        if (index != null && !index.isEmpty()) {
            mappingsRequest.indices(index);
        }

        return client
                .admin()
                .indices()
                .getMappings(mappingsRequest)
                .get()
                .getMappings();
    }

    Set<ElasticsearchColumn> getColumns(ElasticsearchTableSource src)
            throws ExecutionException, InterruptedException, IOException, JSONException
    {
        Set<ElasticsearchColumn> result = new HashSet();
        String type = src.getType();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> allMappings = getMappings(src);

        // what makes sense is to get the reunion of all the columns from all the mappings for the specified document type
        for (ObjectCursor<String> currentIndex : allMappings.keys()) {
            MappingMetaData mappingMetaData = allMappings.get(currentIndex.value).get(type);
            JSONObject json = new JSONObject(mappingMetaData.source().toString())
                    .getJSONObject(type)
                    .getJSONObject("properties");

            List<String> allColumnMetadata = getColumnsMetadata(null, json);
            for (String columnMetadata : allColumnMetadata) {
                ElasticsearchColumn clm = createColumn(columnMetadata);
                if (!(clm == null) && !result.contains(clm)) {
                    result.add(clm);
                }
            }
        }

        result.add(createColumn("_id.type:string"));
        result.add(createColumn("_index.type:string"));

        return result;
    }

    List<String> getColumnsMetadata(String parent, JSONObject json)
            throws JSONException
    {
        List<String> leaves = new ArrayList();

        Iterator it = json.keys();
        while (it.hasNext()) {
            String key = (String) it.next();
            Object child = json.get(key);
            String childKey = parent == null || parent.isEmpty() ? key : parent.concat(".").concat(key);

            if (child instanceof JSONObject) {
                leaves.addAll(getColumnsMetadata(childKey, (JSONObject) child));
            }
            else if (child instanceof JSONArray) {
                // ignoring arrays for now
                continue;
            }
            else {
                leaves.add(childKey.concat(":").concat(child.toString()));
            }
        }

        return leaves;
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
                prestoType = VARCHAR; //JSON
                break;
            default:
                prestoType = VARCHAR; //JSON
                break;
        }

        return prestoType;
    }

    ElasticsearchColumn createColumn(String type)
            throws JSONException, IOException
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
                prestoType = VARCHAR; //JSON
                break;
            default:
                prestoType = VARCHAR; //JSON
                break;
        }

        return new ElasticsearchColumn("", prestoType, "", type);
    }

    //    void updateTableColumns(ElasticsearchTable table)
//    {
//        Set<ElasticsearchColumn> columns = new HashSet();
//        for (ElasticsearchTableSource src : table.getSources()) {
//            try {
//                columns.addAll(getColumns(src));
//            }
//            catch (ExecutionException | InterruptedException | IOException | JSONException e) {
//                e.printStackTrace();
//            }
//        }
//
//        table.setColumns(columns
//                .stream()
//                .collect(Collectors.toList()));
//        table.setColumnsMetadata(columns
//                .stream()
//                .map(ElasticsearchColumnMetadata::new)
//                .collect(Collectors.toList()));
//    }
}
