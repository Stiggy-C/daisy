package io.openenterprise.daisy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Map;

public abstract class AbstractApplication {

    @Inject
    protected SparkSession sparkSession;

    @Nonnull
    protected abstract Dataset<Row> buildDataset(@Nonnull Map<String, ?> parameters);
}
