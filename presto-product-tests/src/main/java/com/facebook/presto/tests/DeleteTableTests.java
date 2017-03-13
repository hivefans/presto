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
package com.facebook.presto.tests;

import com.teradata.tempto.AfterTestWithContext;
import com.teradata.tempto.BeforeTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requires;
import com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.DELETE_RECORD;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

@Requires(ImmutableNationTable.class)
public class DeleteTableTests
        extends ProductTest
{
    private static final String DELETE_TABLE_NAME = "delete_table_name";
    private static final String TRUNCATE_TABLE_NAME = "truncate_table_name";

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTables()
    {
        try {
            query(format("DROP TABLE IF EXISTS %s", DELETE_TABLE_NAME));
            query(format("DROP TABLE IF EXISTS %s", TRUNCATE_TABLE_NAME));
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
    }

    @Test(groups = {DELETE_RECORD, SMOKE})
    public void dropTable()
    {
        query(format("CREATE TABLE %s AS SELECT * FROM nation", DELETE_TABLE_NAME));
        assertThat(query(format("SELECT * FROM %s", DELETE_TABLE_NAME)))
                .hasRowsCount(25);

        assertThat(query(format("DELETE FROM %s", DELETE_TABLE_NAME)))
                .hasRowsCount(0);
    }

    @Test(groups = {DELETE_RECORD, SMOKE})
    public void truncateTable()
    {
        query(format("CREATE TABLE %s AS SELECT * FROM nation", TRUNCATE_TABLE_NAME));
        assertThat(query(format("SELECT * FROM %s", TRUNCATE_TABLE_NAME)))
                .hasRowsCount(25);

        assertThat(query(format("TRUNCATE TABLE %s", TRUNCATE_TABLE_NAME)))
                .hasRowsCount(0);
    }
}
