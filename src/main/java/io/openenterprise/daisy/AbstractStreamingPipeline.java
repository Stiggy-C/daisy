package io.openenterprise.daisy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * The base of a streaming data pipeline to be run on an Apache Spark cluster.
 */
public abstract class AbstractStreamingPipeline extends AbstractSparkApplication {

    /**
     * Start this streaming pipeline on the given Apache Spark cluster
     *
     * @param parameters
     */
    public StreamingQuery start(@Nonnull Map<String, ?> parameters) throws TimeoutException {
        Dataset<Row> dataset = buildDataset(parameters);

        return writeDataset(dataset, parameters);
    }

    /**
     * Stream the data of the (aggregated) dataset to desired data source. Need to be filled in by the implementation.
     *
     * @param dataset
     * @param parameters
     * @return
     * @throws TimeoutException
     */
    protected abstract StreamingQuery writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) throws TimeoutException;
}
