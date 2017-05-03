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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_SCHEMA1;
import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_SCHEMA2;
import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_TBL_1;
import static com.facebook.presto.elasticsearch.ElasticsearchTestConstants.ES_TBL_2;
import static org.testng.Assert.assertNotNull;

public class TestElasticsearchClient
{
    public static final int EXPECTED_NR_OF_COLUMNS = 4;
    private ElasticsearchClient client;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        client = new ElasticsearchClient(new ElasticsearchConfig());
    }

    @Test
    public void testSchema()
            throws Exception
    {
//        Map<String, Map<String, ElasticsearchTable>> schemas = client.updateSchemas();
//        assertNotNull(schemas);
    }

    @Test
    public void testTable()
            throws Exception
    {
        ElasticsearchTable table = client.getTable(ES_SCHEMA1, ES_TBL_1);
        assertNotNull(table);
        assertNotNull(table.getColumnMetadatas());
        // assertEquals(table.getColumns().size(), EXPECTED_NR_OF_COLUMNS);
    }

    @Test
    public void testTable2()
        throws Exception
    {
        ElasticsearchTable table = client.getTable(ES_SCHEMA2, ES_TBL_2);
        assertNotNull(table);
        assertNotNull(table.getColumnMetadatas());
        // assertEquals(table.getColumns().size(), EXPECTED_NR_OF_COLUMNS);
    }
}
