package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Parameters;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class SqlStatementComponent extends AbstractBaseDatasetComponentImpl {

    @NotNull
    @Override
    public Dataset<Row> buildDataset(@NotNull Map<String, ?> parameters,
                                     @Nullable CreateTableOrViewPreference createTableOrViewPreference) throws AnalysisException {
        throw new UnsupportedOperationException("Run DDL & DML statements directly instead");
    }

    @NotNull
    @Override
    public Dataset<Row> buildDataset(@NotNull Map<String, ?> parameters) {
        var sql = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                        Parameters.DATASET_SQL_STATEMENT.getName()), "SQL statement is empty");

        return sparkSession.sql(sql);
    }
}
