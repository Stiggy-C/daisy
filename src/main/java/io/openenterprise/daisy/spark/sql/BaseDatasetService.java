package io.openenterprise.daisy.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * The base of all Apache Spark {@link Dataset} operations.
 */
public interface BaseDatasetService {

    Dataset<Row> buildDataset(@Nonnull Map<String, ?> parameters);
}
