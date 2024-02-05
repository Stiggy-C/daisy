package io.openenterprise.daisy.spark.sql.execution.datasources.jdbc;


import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import scala.Option;
import scala.collection.immutable.Map;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.stream.Collectors;

public class MySqlJdbcRelationProvider extends AbstractUpsertJdbcRelationProvider {

    public static final String SHORT_NAME = "mysql";

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

    @NotNull
    @Override
    protected String getUpsertStatement(@Nonnull String table, @Nonnull StructType schema,
                                        @Nonnull Option<StructType> tableSchema, boolean isCaseSensitive,
                                        @Nonnull JdbcDialect jdbcDialect, @Nonnull Map<String, String> parameters) {
        var dialect = JdbcDialects.get(JdbcOptionsInWrite.JDBC_URL());
        var baseStatement = JdbcUtils.getInsertStatement(table, schema, tableSchema, isCaseSensitive, jdbcDialect);

        return baseStatement + " ON DUPLICATE KEY UPDATE " + Arrays.stream(schema.fieldNames())
                .map(dialect::quoteIdentifier).map(field -> field + " = ?").collect(Collectors.joining(","));
    }
}
