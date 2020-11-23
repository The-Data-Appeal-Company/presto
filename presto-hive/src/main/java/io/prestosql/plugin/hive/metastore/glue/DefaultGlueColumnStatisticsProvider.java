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

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.concurrent.MoreFutures;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HiveColumnStatistics;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.glue.converter.GlueStatConverter;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;

public class DefaultGlueColumnStatisticsProvider
        implements GlueColumnStatisticsProvider {

    private static final int GLUE_COLUMN_READ_STAT_PAGE_SIZE = 100;
    private static final int GLUE_COLUMN_WRITE_STAT_PAGE_SIZE = 25;

    private final AWSGlueAsync glueClient;
    private final String catalogId;
    private final Executor readExecutor;
    private final Executor writeExecutor;

    public DefaultGlueColumnStatisticsProvider(AWSGlueAsync glueClient, String catalogId, Executor readExecutor, Executor writeExecutor) {
        this.glueClient = glueClient;
        this.catalogId = catalogId;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type) {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(Table table) {
        final List<String> columnNames = getAllColumns(table);
        final List<List<String>> columnChunks = Lists.partition(columnNames, GLUE_COLUMN_READ_STAT_PAGE_SIZE);

        ImmutableList.Builder<CompletableFuture<GetColumnStatisticsForTableResult>> getStatsFutures = ImmutableList.builder();

        columnChunks.forEach(partialColumns -> getStatsFutures.add(CompletableFuture.supplyAsync(() -> {
            final GetColumnStatisticsForTableRequest request = new GetColumnStatisticsForTableRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(table.getDatabaseName())
                    .withTableName(table.getTableName())
                    .withColumnNames(partialColumns);

            return glueClient.getColumnStatisticsForTable(request);
        }, readExecutor)));


        final ImmutableList.Builder<ColumnStatistics> columnStatsBuilder = ImmutableList.builder();
        for (CompletableFuture<GetColumnStatisticsForTableResult> future : getStatsFutures.build()) {
            final GetColumnStatisticsForTableResult tableColumnsStats = MoreFutures.getFutureValue(future, PrestoException.class);
            columnStatsBuilder.addAll(tableColumnsStats.getColumnStatisticsList());
        }

        final HiveBasicStatistics tableStatistics = getHiveBasicStatistics(table.getParameters());

        final ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
        columnStatsBuilder.build().forEach(col -> columnStatsMapBuilder.put(col.getColumnName(),
                GlueStatConverter.fromGlueColumnStatistics(col.getStatisticsData(), tableStatistics.getRowCount()))
        );

        return columnStatsMapBuilder.build();
    }

    @Override
    public Map<String, HiveColumnStatistics> getPartitionColumnStatistics(Partition partition) {
        final List<List<Column>> columnChunks = Lists.partition(partition.getColumns(), GLUE_COLUMN_READ_STAT_PAGE_SIZE);
        ImmutableList.Builder<CompletableFuture<GetColumnStatisticsForPartitionResult>> getStatsFutures = ImmutableList.builder();

        columnChunks.forEach(partialColumns -> getStatsFutures.add(CompletableFuture.supplyAsync(() -> {
            final List<String> columnsNames = partialColumns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toUnmodifiableList());

            final GetColumnStatisticsForPartitionRequest request = new GetColumnStatisticsForPartitionRequest()
                    .withCatalogId(catalogId)
                    .withDatabaseName(partition.getDatabaseName())
                    .withTableName(partition.getTableName())
                    .withColumnNames(columnsNames)
                    .withPartitionValues(partition.getValues());

            return glueClient.getColumnStatisticsForPartition(request);
        }, readExecutor)));


        final ImmutableList.Builder<ColumnStatistics> columnStatsBuilder = ImmutableList.builder();
        for (CompletableFuture<GetColumnStatisticsForPartitionResult> future : getStatsFutures.build()) {
            final GetColumnStatisticsForPartitionResult tableColumnsStats = MoreFutures.getFutureValue(future, PrestoException.class);
            columnStatsBuilder.addAll(tableColumnsStats.getColumnStatisticsList());
        }

        final HiveBasicStatistics tableStatistics = getHiveBasicStatistics(partition.getParameters());

        final ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
        columnStatsBuilder.build().forEach(col -> columnStatsMapBuilder.put(col.getColumnName(),
                GlueStatConverter.fromGlueColumnStatistics(col.getStatisticsData(), tableStatistics.getRowCount()))
        );

        return columnStatsMapBuilder.build();
    }

    @Override
    public void updateTableColumnStatistics(Table table, Map<String, HiveColumnStatistics> columnStatistics) {
        final HiveBasicStatistics tableStats = getHiveBasicStatistics(table.getParameters());

        if (columnStatistics.isEmpty()) {
            final List<CompletableFuture<Void>> writeFutures = Stream.concat(table.getDataColumns().stream(), table.getPartitionColumns().stream())
                    .map(column -> CompletableFuture.runAsync(() -> glueClient.deleteColumnStatisticsForTable(
                            new DeleteColumnStatisticsForTableRequest()
                                    .withCatalogId(catalogId)
                                    .withDatabaseName(table.getDatabaseName())
                                    .withTableName(table.getTableName())
                                    .withColumnName(column.getName())), writeExecutor))
                    .collect(Collectors.toUnmodifiableList());

            for (CompletableFuture<Void> writeFuture : writeFutures) {
                MoreFutures.getFutureValue(writeFuture);
            }
        }

        final List<ColumnStatistics> columnStats = GlueStatConverter.toGlueColumnStatistics(table, columnStatistics, tableStats.getRowCount());
        final List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

        final List<CompletableFuture<Void>> writeChunkFutures = columnChunks.stream().map(columnChunk -> CompletableFuture.runAsync(
                () -> glueClient.updateColumnStatisticsForTable(
                        new UpdateColumnStatisticsForTableRequest()
                                .withCatalogId(catalogId)
                                .withDatabaseName(table.getDatabaseName())
                                .withTableName(table.getTableName())
                                .withColumnStatisticsList(columnChunk)
                )
        )).collect(Collectors.toUnmodifiableList());

        for (CompletableFuture<Void> writeChunkFuture : writeChunkFutures) {
            MoreFutures.getFutureValue(writeChunkFuture);
        }

    }

    @Override
    public void updatePartitionStatistics(Partition partition, Map<String, HiveColumnStatistics> columnStatistics, boolean forPartitionCreation) {
        if (columnStatistics.isEmpty() && !forPartitionCreation) {

            final List<CompletableFuture<Void>> writeFutures = partition.getColumns().stream()
                    .map(column -> CompletableFuture.runAsync(() ->
                            glueClient.deleteColumnStatisticsForPartition(
                                    new DeleteColumnStatisticsForPartitionRequest()
                                            .withCatalogId(catalogId)
                                            .withDatabaseName(partition.getDatabaseName())
                                            .withTableName(partition.getTableName())
                                            .withPartitionValues(partition.getValues())
                                            .withColumnName(column.getName())), writeExecutor)
                    ).collect(Collectors.toUnmodifiableList());

            for (CompletableFuture<Void> writeFuture : writeFutures) {
                MoreFutures.getFutureValue(writeFuture);
            }
        }


        final HiveBasicStatistics partitionStats = getHiveBasicStatistics(partition.getParameters());
        final List<ColumnStatistics> columnStats = GlueStatConverter.toGlueColumnStatistics(partition, columnStatistics, partitionStats.getRowCount());

        final List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

        final List<CompletableFuture<Void>> writePartitionStatsFutures = columnChunks.stream()
                .map(column ->
                        CompletableFuture.runAsync(() -> glueClient.updateColumnStatisticsForPartition(new UpdateColumnStatisticsForPartitionRequest()
                                .withCatalogId(catalogId)
                                .withDatabaseName(partition.getDatabaseName())
                                .withTableName(partition.getTableName())
                                .withPartitionValues(partition.getValues())
                                .withColumnStatisticsList(column)), writeExecutor)
                ).collect(Collectors.toUnmodifiableList());

        for (CompletableFuture<Void> writePartitionStatsFuture : writePartitionStatsFutures) {
            MoreFutures.getFutureValue(writePartitionStatsFuture);
        }
    }

    private List<String> getAllColumns(Table table) {
        final List<String> allColumns = new ArrayList<>(table.getDataColumns().size() + table.getPartitionColumns().size());
        table.getDataColumns().stream().map(Column::getName).forEach(allColumns::add);
        table.getPartitionColumns().stream().map(Column::getName).forEach(allColumns::add);
        return allColumns;
    }

}
