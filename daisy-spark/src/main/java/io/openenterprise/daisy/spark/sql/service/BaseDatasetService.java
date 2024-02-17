package io.openenterprise.daisy.spark.sql.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface BaseDatasetService<T> {

    String DEFAULT_FORMAT = DeltaSourceUtils.NAME();

    @Nullable
    Dataset<Row> loadDataset(@Nonnull Map<String, Object> parameters);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String format, @Nullable Map<String, String> options, @Nonnull String path);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String tableOrView);

    T saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters) throws Exception;
}
