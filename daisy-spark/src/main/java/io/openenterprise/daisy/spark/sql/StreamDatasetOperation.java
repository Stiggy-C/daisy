package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Operation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import javax.annotation.Nonnull;
import java.util.Map;

public interface StreamDatasetOperation extends Operation<StreamingQuery> {

    StreamingQuery stream(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters);
}
