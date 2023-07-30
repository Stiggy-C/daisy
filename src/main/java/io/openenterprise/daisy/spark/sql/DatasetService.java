package io.openenterprise.daisy.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.Map;

public interface DatasetService extends BaseDatasetService {

    void writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);
}
