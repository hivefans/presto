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

package com.facebook.presto.tests.cassandra;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.CASSANDRA;
import static com.facebook.presto.tests.cassandra.DataTypesTableDefinition.CASSANDRA_ALL_TYPES;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.util.DateTimeUtils.parseTimestampInUTC;
import static java.lang.String.format;

public class TestInsertIntoCassandraTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String CASSANDRA_INSERT_TABLE = "t1";

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return mutableTable(CASSANDRA_ALL_TYPES, CASSANDRA_INSERT_TABLE, CREATED);
    }

    @Test(groups = CASSANDRA)
    public void testInsertIntoValuesToCassandraTableAllSimpleTypes()
            throws SQLException
    {
        String tableNameInDatabase = String.format("%s.%s",
                mutableTablesState().get(CASSANDRA_INSERT_TABLE).getDatabase(),
                mutableTablesState().get(CASSANDRA_INSERT_TABLE).getNameInDatabase());

        QueryResult queryResult = query("SELECT * FROM " + tableNameInDatabase);
        assertThat(queryResult).hasNoRows();

        //todo: Following types are not supported now. We need to change null into the value after fixing it
        // blob, frozen<set<type>>, inet, list<type>, map<type,type>, set<type>, timeuuid
        // decimal, uuid, varint can be inserted but the expected and actual values are not same
        query("INSERT INTO " + tableNameInDatabase +
                "(a, b, bl, bo, d, do, f, fr, i, integer, l, m, s, t, ti, tu, u, v, vari) VALUES (" +
                "'ascii value', " +
                "BIGINT '99999', " +
                "null, " +
                "true, " +
                "123.456, " +
                "123.456789, " +
                "REAL '123.45678', " +
                "null, " +
                "null, " +
                "123, "  +
                "null, "  +
                "null, "  +
                "null, "  +
                "'text value', " +
                "timestamp '9999-12-31 23:59:59'," +
                "null, " +
                "'d2177dd0-eaa2-11', " +
                "'varchar value'," +
                "'varint value')");

        assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        "ascii value",
                        99999,
                        null,
                        true,
                        0.0,
                        123.456789,
                        123.45678,
                        null,
                        null,
                        123,
                        null,
                        null,
                        null,
                        "text value",
                        parseTimestampInUTC("9999-12-31 23:59:59"),
                        null,
                        "64323137-3764-6430-2d65-6161322d3131",
                        "varchar value",
                        "36637037258067534152811181413"));

        // insert null for all datatypes
        query("INSERT INTO " + tableNameInDatabase +
                "(a, b, bl, bo, d, do, f, fr, i, integer, l, m, s, t, ti, tu, u, v, vari) VALUES (" +
                "'key 1', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null) ");
        assertThat(query(format("SELECT * FROM %s WHERE a = 'key 1'", tableNameInDatabase))).containsOnly(
                row("key 1", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));

        // insert into only a subset of columns
        query(format("INSERT INTO %s (a, bo, integer, t) VALUES ('key 2', false, 999, 'text 2')", tableNameInDatabase));
        assertThat(query(format("SELECT * FROM %s WHERE a = 'key 2'", tableNameInDatabase))).containsOnly(
                row("key 2", null, null, false, null, null, null, null, null, 999, null, null, null, "text 2", null, null, null, null, null));

        // negative test: failed to insert null to primary key
        assertThat(() -> query(format("INSERT INTO %s (a) VALUES (null) ", tableNameInDatabase)))
                .failsWithMessage("Invalid null value in condition for column a");
    }
}
