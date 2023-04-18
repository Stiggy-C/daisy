package io.openenterprise.daisy.spark.api.java.function;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.shaded.com.nimbusds.jose.util.ArrayUtils;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@NoArgsConstructor
public class ForeachJdbcWriter extends ForeachWriter<Row> implements ForeachFunction<Row> {

    private static final Map<Integer, DataSource> DATA_SOURCES = new HashMap<>();

    @Setter
    private JdbcOptionsInWrite jdbcOptionsInWrite;


    private transient volatile Boolean dbTableExists;

    @Setter
    private String password;

    @Setter
    private String username;


    @Setter
    private boolean upsert;

    @Override
    public void call(Row row) {
        var dataSource = getDataSource();
        var jdbcTemplate = new JdbcTemplate(dataSource);

        if (Objects.isNull(dbTableExists)) {
            try {
                dbTableExists = JdbcUtils.tableExists(dataSource.getConnection(), jdbcOptionsInWrite);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        if (!dbTableExists) {
            try {
                JdbcUtils.createTable(dataSource.getConnection(), jdbcOptionsInWrite.table(), row.schema(),
                        false, jdbcOptionsInWrite);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            dbTableExists = true;
        }

        var dialect = JdbcDialects.get(JdbcOptionsInWrite.JDBC_URL());
        var schema = row.schema();
        var fieldNames = schema.names();
        var columns = Arrays.stream(fieldNames).map(dialect::quoteIdentifier).collect(Collectors.joining(","));
        var variables = Arrays.stream(fieldNames).map(fieldName -> "?").collect(Collectors.joining(","));

        var sql = "INSERT INTO " + jdbcOptionsInWrite.table() + "(" + columns + ") VALUES (" + variables + ")";

        if (upsert) {
            if (jdbcOptionsInWrite.driverClass().equals(com.mysql.cj.jdbc.Driver.class.getName())
                    || jdbcOptionsInWrite.driverClass().equals(com.mysql.jdbc.Driver.class.getName())) {
                sql += " ON DUPLICATE KEY UPDATE ";
            } else if (jdbcOptionsInWrite.driverClass().equals(org.postgresql.Driver.class.getName())) {
                sql += " ON CONFLICT DO UPDATE SET ";
            } else {
                throw new UnsupportedOperationException();
            }

            sql += Arrays.stream(fieldNames).map(fieldName -> fieldName + " = ?").collect(Collectors.joining(","));
        }

        var values = Arrays.stream(schema.fields())
                .map(field -> row.get(row.fieldIndex(field.name()))).toArray();
        var args = upsert ? ArrayUtils.concat(values, values) : values;

        jdbcTemplate.update(sql, args);
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        return true;
    }

    @Override
    public void process(Row row) {
        this.call(row);
    }

    @Override
    public void close(Throwable errorOrNull) {
        // Do nothing
    }

    protected DataSource getDataSource() {
        DataSource dataSource;
        int dataSourceKey = Objects.hash(jdbcOptionsInWrite.driverClass(), jdbcOptionsInWrite.url());

        synchronized (DATA_SOURCES) {
            if (DATA_SOURCES.containsKey(dataSourceKey)) {
                dataSource = DATA_SOURCES.get(dataSourceKey);
            } else {
                var baseDataSource = DataSourceBuilder.create().driverClassName(jdbcOptionsInWrite.driverClass())
                        .password(password).username(username).url(jdbcOptionsInWrite.url()).build();

                var hikariConfig = new HikariConfig();
                hikariConfig.setDataSource(baseDataSource);

                dataSource = new HikariDataSource(hikariConfig);

                DATA_SOURCES.put(dataSourceKey, dataSource);
            }
        }

        return dataSource;
    }
}
