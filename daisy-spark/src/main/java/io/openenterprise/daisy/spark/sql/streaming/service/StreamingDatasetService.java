package io.openenterprise.daisy.spark.sql.streaming.service;

import io.openenterprise.daisy.spark.sql.service.BaseDatasetService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface StreamingDatasetService extends BaseDatasetService<StreamingQuery> {

    @Nonnull
    StreamingQuery saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String table, @Nullable String format,
                               @Nullable Map<String, String> options, @Nullable OutputMode outputMode)
            throws TimeoutException;

    @Nonnull
    StreamingQuery saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String path, @Nullable String format,
                                         @Nullable Map<String, String> options, @Nullable OutputMode outputMode);

}
