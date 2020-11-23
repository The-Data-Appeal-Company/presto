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
package io.prestosql.plugin.hive.metastore.glue;

import io.prestosql.plugin.hive.metastore.HiveColumnStatistics;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;

import java.util.Map;
import java.util.Set;

public interface GlueColumnStatisticsProvider
{
    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    Map<String, HiveColumnStatistics> getTableColumnStatistics(Table table);

    Map<String, HiveColumnStatistics> getPartitionColumnStatistics(Partition partition);

    void updateTableColumnStatistics(Table table, Map<String, HiveColumnStatistics> columnStatistics);

    void updatePartitionStatistics(Partition partition, Map<String, HiveColumnStatistics> columnStatistics, boolean forPartitionCreation);
}
