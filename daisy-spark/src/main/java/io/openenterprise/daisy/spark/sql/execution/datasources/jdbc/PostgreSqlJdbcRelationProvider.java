package io.openenterprise.daisy.spark.sql.execution.datasources.jdbc;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.immutable.Map;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.stream.Collectors;

public class PostgreSqlJdbcRelationProvider extends AbstractUpsertJdbcRelationProvider{

    public static final String SHORT_NAME = "postgresql";

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

    @Nonnull
    @Override
    protected String getUpsertStatement(@Nonnull String table, @Nonnull StructType schema,
                                        @Nonnull Option<StructType> tableSchema, boolean isCaseSensitive,
                                        @Nonnull JdbcDialect jdbcDialect, @Nonnull Map<String, String> parameters) {
        var baseStatement = JdbcUtils.getInsertStatement(table, schema, tableSchema, isCaseSensitive, jdbcDialect);
        var primaryKeys = StringUtils.split(parameters.getOrElse(JDBCOptions.PRIMARY_KEYS.getValue(),
                () -> null), ",");
        var primaryKeyColumns = ArrayUtils.isEmpty(primaryKeys)?  "" : "(" + Arrays.stream(primaryKeys)
                .map(StringUtils::trim).map(jdbcDialect::quoteIdentifier).collect(Collectors.joining(",")) + ")";

        return baseStatement + " ON CONFLICT " +  primaryKeyColumns + " DO UPDATE SET " +
                Arrays.stream(schema.fieldNames()).map(jdbcDialect::quoteIdentifier).map(field -> field + " = ?")
                        .collect(Collectors.joining(","));
    }
}
