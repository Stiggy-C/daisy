package io.openenterprise.daisy.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

public abstract class AbstractStreamDatasetOperationImpl implements StreamDatasetOperation {

    @Nullable
    @Override
    public StreamingQuery invoke(@NotNull Map<String, Object> parameters) {
        var dataset = getDataset(parameters);
        if (Objects.isNull(dataset)) {
            throw new IllegalStateException("dataset can not be null");
        }

        return stream(dataset, parameters);
    }

    @Nullable
    protected abstract Dataset<Row> getDataset(@NotNull Map<String, Object> parameters);
}
