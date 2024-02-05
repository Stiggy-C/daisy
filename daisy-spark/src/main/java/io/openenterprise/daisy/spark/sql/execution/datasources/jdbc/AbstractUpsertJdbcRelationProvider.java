package io.openenterprise.daisy.spark.sql.execution.datasources.jdbc;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractUpsertJdbcRelationProvider extends JdbcRelationProvider {

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> df) {
        var isCaseSensitive = sqlContext.conf().caseSensitiveAnalysis();
        var jdbcOptions = new JdbcOptionsInWrite(parameters);
        var jdbcDialect = JdbcDialects.get(jdbcOptions.url());
        var jdbcConnectionFactory = jdbcDialect.createConnectionFactory(jdbcOptions);
        var jdbcConnection = jdbcConnectionFactory.apply(-1);
        var tableSchema = JdbcUtils.getSchemaOption(jdbcConnection, jdbcOptions);
        var tableExists = JdbcUtils.tableExists(jdbcConnection, jdbcOptions);

        if (!tableExists) {
            JdbcUtils.createTable(jdbcConnection, jdbcOptions.table(), df.schema(), isCaseSensitive, jdbcOptions);

            var primaryKeys = StringUtils.split(parameters.getOrElse(JDBCOptions.PRIMARY_KEYS.getValue(),
                    () -> null), ",");

            if (ArrayUtils.isNotEmpty(primaryKeys)) {
                var columns = new NamedReference[primaryKeys.length];

                for (var i = 0; i < primaryKeys.length; i++) {
                    columns[i] = FieldReference.column(StringUtils.trim(primaryKeys[i]));
                }

                var createIndexStatement = jdbcDialect.createIndex("pk_" + jdbcOptions.table(),
                        Identifier.of(new String[0], jdbcOptions.table()), columns, Collections.emptyMap(), Collections.emptyMap());
                createIndexStatement = StringUtils.replace(createIndexStatement, "CREATE INDEX",
                        "CREATE UNIQUE INDEX");

                try {
                    jdbcConnection.createStatement().executeUpdate(createIndexStatement);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (Objects.equals(SaveMode.Append, mode)) {
            var upsert = BooleanUtils.toBoolean(parameters.getOrElse(JDBCOptions.UPSERT.getValue(), () -> "false"));

            if (upsert) {
                /*
                    In order to re-use org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.savePartition, need to
                    select all columns twice and alias the columns with something different when select again.
                 */
                var columns0 = Arrays.stream(df.schema().fields()).map(StructField::name).map(Column::new)
                        .collect(Collectors.toList());
                var columns1 = Arrays.stream(df.schema().fields()).map(StructField::name)
                        .map(fieldName -> new Column(fieldName).as(fieldName + "1"))
                        .collect(Collectors.toList());
                var columnsUnion = new ArrayList<>(columns0);

                CollectionUtils.addAll(columnsUnion, columns1);

                var upsertDf = df.select(columnsUnion.toArray(new Column[]{}));
                var upsertStatement = getUpsertStatement(jdbcOptions.table(), df.schema(), tableSchema,
                        isCaseSensitive, jdbcDialect, parameters);

                var foreachPartitionFunction = new ForeachPartitionFunction(jdbcDialect, jdbcOptions, upsertDf.schema(),
                        upsertStatement);

                upsertDf.foreachPartition(foreachPartitionFunction);

                return super.createRelation(sqlContext, parameters);
            }
        }

        return super.createRelation(sqlContext, mode, parameters, df);
    }

    @Nonnull
    protected abstract String getUpsertStatement(
            @Nonnull String table, @Nonnull StructType dfSchema, @Nonnull Option<StructType> tableSchema,
            boolean isCaseSensitive, @Nonnull JdbcDialect jdbcDialect, @Nonnull Map<String, String> parameters) ;

    protected static class ForeachPartitionFunction
            implements org.apache.spark.api.java.function.ForeachPartitionFunction<Row> {

        protected JdbcDialect jdbcDialect;

        protected JdbcOptionsInWrite jdbcOptions;

        protected StructType schema;

        protected String sqlStatement;

        public ForeachPartitionFunction(@Nonnull JdbcDialect jdbcDialect, @Nonnull JdbcOptionsInWrite jdbcOptions,
                                        @Nonnull StructType schema, @Nonnull String sqlStatement) {
            this.jdbcDialect = jdbcDialect;
            this.jdbcOptions = jdbcOptions;
            this.schema = schema;
            this.sqlStatement = sqlStatement;
        }

        @Override
        public void call(Iterator<Row> iterator) throws Exception {
            JdbcUtils.savePartition(jdbcOptions.table(), JavaConverters.asScalaIterator(iterator), schema, sqlStatement,
                    jdbcOptions.batchSize(), jdbcDialect, jdbcOptions.isolationLevel(), jdbcOptions);
        }
    }
}
