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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.List;

import static com.facebook.presto.elasticsearch.EmbeddedElasticsearch.HOST;
import static com.facebook.presto.elasticsearch.EmbeddedElasticsearch.PORT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestElasticsearchConnector
{
    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;
    private static final String DATABASE_NAME = "test";
    private static final String TABLE_NAME = "product";
    private static ElasticsearchClient client;

    @BeforeClass
    public void setup()
            throws Exception
    {
        EmbeddedElasticsearch.start();

        String connectorId = "elasticsearch-test";
        ElasticsearchConnectorFactory connectorFactory = new ElasticsearchConnectorFactory(
                ImmutableMap.of(),
                Thread.currentThread().getContextClassLoader()
        );

        Connector connector = connectorFactory.create(
                connectorId,
                ImmutableMap.of(
                        "connection-hostname", HOST,
                        "connection-port", PORT.toString()),
                new TestingConnectorContext());

        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-admin-indices.html
        URL metadataUrl = Resources.getResource(TestElasticsearchClient.class, "/definition.json");
        String definition = Resources.toString(metadataUrl, UTF_8);
        EmbeddedElasticsearch.getClient().admin().indices()
                .prepareCreate(DATABASE_NAME)
                .addMapping(TABLE_NAME, definition)
                .get();

        client = new ElasticsearchClient(EmbeddedElasticsearch.getClient());
        metadata = connector.getMetadata(ElasticsearchTransactionHandle.INSTANCE);

        splitManager = connector.getSplitManager();

        recordSetProvider = connector.getRecordSetProvider();

        database = ElasticsearchTestConstants.ES_SCHEMA1;
        table = new SchemaTableName(database, ElasticsearchTestConstants.ES_TBL_1);
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        assertEquals(ImmutableList.of(DATABASE_NAME), client.getSchemaNames());
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        assertEquals(ImmutableList.of(TABLE_NAME), client.getAllTables(DATABASE_NAME));
        assertEquals(ImmutableList.of(), client.getAllTables("invalid_database"));
    }

    @Test
    public void testGetRecords()
            throws Exception
    {
    }

    private static void assertReadFields(RecordCursor cursor, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            if (!cursor.isNull(columnIndex)) {
                Type type = column.getType();
                if (BOOLEAN.equals(type)) {
                    cursor.getBoolean(columnIndex);
                }
                else if (INTEGER.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (BIGINT.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (TIMESTAMP.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (DOUBLE.equals(type)) {
                    cursor.getDouble(columnIndex);
                }
                else if (isVarcharType(type) || VARBINARY.equals(type)) {
                    try {
                        cursor.getSlice(columnIndex);
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("column " + column, e);
                    }
                }
                else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(1000)));
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            String name = Types.checkType(columnHandle, ElasticsearchColumnHandle.class, "columnHandle").getColumnName();
            index.put(name, i);
            i++;
        }
        return index.build();
    }
}
