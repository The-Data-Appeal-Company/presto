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
package io.prestosql.plugin.hive.metastore.glue.converter;

import com.amazonaws.services.glue.model.*;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HiveColumnStatistics;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.prestosql.plugin.hive.metastore.HiveColumnStatistics.*;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.*;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

public class GlueStatConverter {

    private static final long DAY_TO_MILLISECOND_FACTOR = TimeUnit.DAYS.toMillis(1);

    public static List<ColumnStatistics> toGlueColumnStatistics(
            Partition partition, Map<String, HiveColumnStatistics> prestoColumnStats, OptionalLong rowCount) {
        List<ColumnStatistics> catalogColumnStatisticsList = new ArrayList<>(prestoColumnStats.size());

        prestoColumnStats.forEach((columnName, statistics) -> {

            final Optional<Column> column = columnByName(partition, columnName);
            HiveType columnType = column.get().getType();

            ColumnStatistics catalogColumnStatistics = new ColumnStatistics();

            catalogColumnStatistics.setColumnName(columnName);
            catalogColumnStatistics.setColumnType(columnType.toString());
            ColumnStatisticsData catalogColumnStatisticsData = getGlueColumnStatisticsData(statistics, columnType, rowCount);
            catalogColumnStatistics.setStatisticsData(catalogColumnStatisticsData);
            catalogColumnStatistics.setAnalyzedTime(new Date());
            catalogColumnStatisticsList.add(catalogColumnStatistics);
        });

        return catalogColumnStatisticsList;
    }

    public static List<ColumnStatistics> toGlueColumnStatistics(
            Table table, Map<String, HiveColumnStatistics> prestoColumnStats, OptionalLong rowCount) {
        List<ColumnStatistics> catalogColumnStatisticsList = new ArrayList<>(prestoColumnStats.size());

        prestoColumnStats.forEach((columnName, statistics) -> {
            HiveType columnType = table.getColumn(columnName).get().getType();
            ColumnStatistics catalogColumnStatistics = new ColumnStatistics();

            catalogColumnStatistics.setColumnName(columnName);
            catalogColumnStatistics.setColumnType(columnType.toString());
            ColumnStatisticsData catalogColumnStatisticsData = getGlueColumnStatisticsData(statistics, columnType, rowCount);
            catalogColumnStatistics.setStatisticsData(catalogColumnStatisticsData);
            catalogColumnStatistics.setAnalyzedTime(new Date());

            catalogColumnStatisticsList.add(catalogColumnStatistics);
        });

        return catalogColumnStatisticsList;
    }

    private static Optional<Column> columnByName(Partition partition, String columnName) {
        for (Column column : partition.getColumns()) {
            if (column.getName().equals(columnName)) {
                return Optional.of(column);
            }
        }
        return Optional.empty();
    }

    private static Optional<Column> columnByName(TableInput table, String columnName) {
        for (com.amazonaws.services.glue.model.Column column : table.getStorageDescriptor().getColumns()) {
            if (column.getName().equals(columnName)) {
                return Optional.of(GlueToPrestoConverter.convertColumn(column));
            }
        }
        return Optional.empty();
    }


    public static HiveColumnStatistics fromGlueColumnStatistics(ColumnStatisticsData catalogColumnStatisticsData, OptionalLong rowCount) {
        ColumnStatisticsType type = ColumnStatisticsType.fromValue(catalogColumnStatisticsData.getType());
        switch (type) {
            case BINARY:
                BinaryColumnStatisticsData catalogBinaryData = catalogColumnStatisticsData.getBinaryColumnStatisticsData();
                OptionalLong maxColumnLengthOfBinary = OptionalLong.of(catalogBinaryData.getMaximumLength());
                OptionalDouble averageColumnLengthOfBinary = OptionalDouble.of(catalogBinaryData.getAverageLength());
                OptionalLong nullsCountOfBinary = fromMetastoreNullsCount(catalogBinaryData.getNumberOfNulls());
                return createBinaryColumnStatistics(
                        maxColumnLengthOfBinary,
                        getTotalSizeInBytes(averageColumnLengthOfBinary, rowCount, nullsCountOfBinary),
                        nullsCountOfBinary);

            case BOOLEAN:
                BooleanColumnStatisticsData catalogBooleanData = catalogColumnStatisticsData.getBooleanColumnStatisticsData();
                return createBooleanColumnStatistics(
                        OptionalLong.of(catalogBooleanData.getNumberOfTrues()),
                        OptionalLong.of(catalogBooleanData.getNumberOfFalses()),
                        fromMetastoreNullsCount(catalogBooleanData.getNumberOfNulls()));

            case DATE:
                DateColumnStatisticsData catalogDateData = catalogColumnStatisticsData.getDateColumnStatisticsData();
                Optional<LocalDate> minOfDate = dateToLocalDate(catalogDateData.getMinimumValue());
                Optional<LocalDate> maxOfDate = dateToLocalDate(catalogDateData.getMaximumValue());
                OptionalLong nullsCountOfDate = fromMetastoreNullsCount(catalogDateData.getNumberOfNulls());
                OptionalLong distinctValuesCountOfDate = OptionalLong.of(catalogDateData.getNumberOfDistinctValues());
                return createDateColumnStatistics(minOfDate, maxOfDate, nullsCountOfDate, fromMetastoreDistinctValuesCount(distinctValuesCountOfDate, nullsCountOfDate, rowCount));

            case DECIMAL:
                DecimalColumnStatisticsData catalogDecimalData = catalogColumnStatisticsData.getDecimalColumnStatisticsData();
                Optional<BigDecimal> minOfDecimal = glueDecimalToBigDecimal(catalogDecimalData.getMinimumValue());
                Optional<BigDecimal> maxOfDecimal = glueDecimalToBigDecimal(catalogDecimalData.getMaximumValue());
                OptionalLong distinctValuesCountOfDecimal = OptionalLong.of(catalogDecimalData.getNumberOfDistinctValues());
                OptionalLong nullsCountOfDecimal = fromMetastoreNullsCount(catalogDecimalData.getNumberOfNulls());
                return createDecimalColumnStatistics(minOfDecimal, maxOfDecimal, nullsCountOfDecimal, fromMetastoreDistinctValuesCount(distinctValuesCountOfDecimal, nullsCountOfDecimal, rowCount));

            case DOUBLE:
                DoubleColumnStatisticsData catalogDoubleData = catalogColumnStatisticsData.getDoubleColumnStatisticsData();
                OptionalDouble minOfDouble = OptionalDouble.of(catalogDoubleData.getMinimumValue());
                OptionalDouble maxOfDouble = OptionalDouble.of(catalogDoubleData.getMaximumValue());
                OptionalLong nullsCountOfDouble = fromMetastoreNullsCount(catalogDoubleData.getNumberOfNulls());
                OptionalLong distinctValuesCountOfDouble = OptionalLong.of(catalogDoubleData.getNumberOfDistinctValues());
                return createDoubleColumnStatistics(minOfDouble, maxOfDouble, nullsCountOfDouble, fromMetastoreDistinctValuesCount(distinctValuesCountOfDouble, nullsCountOfDouble, rowCount));

            case LONG:
                LongColumnStatisticsData catalogLongData = catalogColumnStatisticsData.getLongColumnStatisticsData();
                OptionalLong minOfLong = OptionalLong.of(catalogLongData.getMinimumValue());
                OptionalLong maxOfLong = OptionalLong.of(catalogLongData.getMaximumValue());
                OptionalLong nullsCountOfLong = fromMetastoreNullsCount(catalogLongData.getNumberOfNulls());
                OptionalLong distinctValuesCountOfLong = OptionalLong.of(catalogLongData.getNumberOfDistinctValues());
                return createIntegerColumnStatistics(minOfLong, maxOfLong, nullsCountOfLong, fromMetastoreDistinctValuesCount(distinctValuesCountOfLong, nullsCountOfLong, rowCount));

            case STRING:
                StringColumnStatisticsData catalogStringData = catalogColumnStatisticsData.getStringColumnStatisticsData();
                OptionalLong maxColumnLengthOfString = OptionalLong.of(catalogStringData.getMaximumLength());
                OptionalDouble averageColumnLengthOfString = OptionalDouble.of(catalogStringData.getAverageLength());
                OptionalLong nullsCountOfString = fromMetastoreNullsCount(catalogStringData.getNumberOfNulls());
                OptionalLong distinctValuesCountOfString = OptionalLong.of(catalogStringData.getNumberOfDistinctValues());

                return createStringColumnStatistics(
                        maxColumnLengthOfString,
                        getTotalSizeInBytes(averageColumnLengthOfString, rowCount, nullsCountOfString),
                        nullsCountOfString,
                        fromMetastoreDistinctValuesCount(distinctValuesCountOfString, nullsCountOfString, rowCount));
        }

        throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + catalogColumnStatisticsData);
    }

    private static ColumnStatisticsData getGlueColumnStatisticsData(HiveColumnStatistics statistics, HiveType columnType, OptionalLong rowCount) {
        TypeInfo typeInfo = columnType.getTypeInfo();
        checkArgument(typeInfo.getCategory() == PRIMITIVE, "unsupported type: %s", columnType);

        ColumnStatisticsData catalogColumnStatisticsData = new ColumnStatisticsData();

        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN:
                BooleanColumnStatisticsData catalogBooleanData = new BooleanColumnStatisticsData();
                statistics.getNullsCount().ifPresent(catalogBooleanData::setNumberOfNulls);
                statistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
                    booleanStatistics.getFalseCount().ifPresent(catalogBooleanData::setNumberOfFalses);
                    booleanStatistics.getTrueCount().ifPresent(catalogBooleanData::setNumberOfTrues);
                });
                catalogColumnStatisticsData.setType(ColumnStatisticsType.BOOLEAN.toString());
                catalogColumnStatisticsData.setBooleanColumnStatisticsData(catalogBooleanData);
                break;
            case BINARY:
                BinaryColumnStatisticsData catalogBinaryData = new BinaryColumnStatisticsData();
                statistics.getNullsCount().ifPresent(catalogBinaryData::setNumberOfNulls);
                catalogBinaryData.setMaximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                catalogBinaryData.setAverageLength(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
                catalogColumnStatisticsData.setType(ColumnStatisticsType.BINARY.toString());
                catalogColumnStatisticsData.setBinaryColumnStatisticsData(catalogBinaryData);
                break;
            case DATE:
                DateColumnStatisticsData catalogDateData = new DateColumnStatisticsData();
                statistics.getDateStatistics().ifPresent(dateStatistics -> {
                    dateStatistics.getMin().ifPresent(value -> catalogDateData.setMinimumValue(localDateToDate(value)));
                    dateStatistics.getMax().ifPresent(value -> catalogDateData.setMaximumValue(localDateToDate(value)));
                });
                statistics.getNullsCount().ifPresent(catalogDateData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogDateData::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DATE.toString());
                catalogColumnStatisticsData.setDateColumnStatisticsData(catalogDateData);
                break;
            case DECIMAL:
                DecimalColumnStatisticsData catalogDecimalData = new DecimalColumnStatisticsData();
                statistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
                    decimalStatistics.getMin().ifPresent(value -> catalogDecimalData.setMinimumValue(bigDecimalToGlueDecimal(value)));
                    decimalStatistics.getMax().ifPresent(value -> catalogDecimalData.setMaximumValue(bigDecimalToGlueDecimal(value)));
                });
                statistics.getNullsCount().ifPresent(catalogDecimalData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogDecimalData::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DECIMAL.toString());
                catalogColumnStatisticsData.setDecimalColumnStatisticsData(catalogDecimalData);
                break;
            case FLOAT:
            case DOUBLE:
                DoubleColumnStatisticsData catalogDoubleData = new DoubleColumnStatisticsData();
                statistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
                    doubleStatistics.getMin().ifPresent(catalogDoubleData::setMinimumValue);
                    doubleStatistics.getMax().ifPresent(catalogDoubleData::setMaximumValue);
                });
                statistics.getNullsCount().ifPresent(catalogDoubleData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogDoubleData::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.DOUBLE.toString());
                catalogColumnStatisticsData.setDoubleColumnStatisticsData(catalogDoubleData);
                break;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP:
                LongColumnStatisticsData catalogLongData = new LongColumnStatisticsData();
                statistics.getIntegerStatistics().ifPresent(integerStatistics -> {
                    integerStatistics.getMin().ifPresent(catalogLongData::setMinimumValue);
                    integerStatistics.getMax().ifPresent(catalogLongData::setMaximumValue);
                });
                statistics.getNullsCount().ifPresent(catalogLongData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogLongData::setNumberOfDistinctValues);
                catalogColumnStatisticsData.setType(ColumnStatisticsType.LONG.toString());
                catalogColumnStatisticsData.setLongColumnStatisticsData(catalogLongData);
                break;
            case VARCHAR:
            case CHAR:
            case STRING:
                StringColumnStatisticsData catalogStringData = new StringColumnStatisticsData();
                statistics.getNullsCount().ifPresent(catalogStringData::setNumberOfNulls);
                toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(catalogStringData::setNumberOfDistinctValues);
                catalogStringData.setMaximumLength(statistics.getMaxValueSizeInBytes().orElse(0));
                catalogStringData.setAverageLength(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
                catalogColumnStatisticsData.setType(ColumnStatisticsType.STRING.toString());
                catalogColumnStatisticsData.setStringColumnStatisticsData(catalogStringData);
                break;
            default:
                throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics type: " + statistics);
        }
        return catalogColumnStatisticsData;
    }


    private static DecimalNumber bigDecimalToGlueDecimal(BigDecimal decimal) {
        Decimal hiveDecimal = new Decimal((short) decimal.scale(), ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
        DecimalNumber catalogDecimal = new DecimalNumber();
        catalogDecimal.setUnscaledValue(ByteBuffer.wrap(hiveDecimal.getUnscaled()));
        catalogDecimal.setScale((int) hiveDecimal.getScale());
        return catalogDecimal;
    }

    private static Optional<BigDecimal> glueDecimalToBigDecimal(DecimalNumber catalogDecimal) {
        Decimal decimal = new Decimal();
        decimal.setUnscaled(catalogDecimal.getUnscaledValue());
        decimal.setScale(catalogDecimal.getScale().shortValue());
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    private static Optional<LocalDate> dateToLocalDate(Date date) {
        long daysSinceEpoch = date.getTime() / DAY_TO_MILLISECOND_FACTOR;
        return Optional.of(LocalDate.ofEpochDay(daysSinceEpoch));
    }

    private static Date localDateToDate(LocalDate date) {
        long millisecondsSinceEpoch = date.toEpochDay() * DAY_TO_MILLISECOND_FACTOR;
        return new Date(millisecondsSinceEpoch);
    }
}
