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
package io.prestosql.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import static io.prestosql.plugin.cassandra.CassandraQueryRunner.createCassandraQueryRunner;

public class TestCassandraDistributedQueries
        extends BaseCassandraDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CassandraServer server = closeAfterClass(new CassandraServer());
        return createCassandraQueryRunner(server, ImmutableMap.of(), TpchTable.getTables());
    }
}
