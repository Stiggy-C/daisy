package io.openenterprise.daisy.spark.sql.streaming;

import io.openenterprise.daisy.Operation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import javax.annotation.Nonnull;
import java.util.Map;

public interface SaveStreamingDatasetOperation extends Operation<StreamingQuery> {
}
