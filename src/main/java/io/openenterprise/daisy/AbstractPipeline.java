package io.openenterprise.daisy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * The base of a data pipeline to be run on an Apache Spark cluster.
 */
public abstract class AbstractPipeline extends AbstractSparkApplication {

    /**
     * Run this pipeline on the given Apache Spark cluster
     *
     * @param parameters
     */
    public void run(@Nonnull Map<String, ?> parameters) {
        Dataset<Row> dataset = buildDataset(parameters);
        writeDataset(dataset, parameters);
    }

    /**
     * Write the aggregated {@link Dataset} to desired data source (say a RDBMS like Postgres). Need to be filled in by
     * the implementation.
     *
     * @param dataset
     * @param parameters
     */
    protected abstract void writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);
}
