package io.openenterprise.daisy.spark.sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * The base of a streaming data pipeline to be run on an Apache Spark cluster.
 */
public abstract class AbstractStreamingDatasetComponentImpl extends AbstractBaseDatasetComponentImpl
    implements StreamingDatasetComponent {

    /**
     * Run this as a streaming pipeline
     *
     * @param parameters
     * @return
     * @throws TimeoutException
     * @throws AnalysisException
     */
    public StreamingQuery streamingPipeline(@Nonnull Map<String, ?> parameters) throws TimeoutException, AnalysisException {
        return streamingPipeline(parameters, null);
    }

    /**
     * Run this as a streaming pipeline and create a temp view of the {@link Dataset} to be re-used
     *
     * @param parameters
     * @param createTableOrViewPreference
     * @return
     * @throws TimeoutException
     * @throws AnalysisException
     */
    public StreamingQuery streamingPipeline(
            @Nonnull Map<String, ?> parameters, @Nullable CreateTableOrViewPreference createTableOrViewPreference)
            throws TimeoutException, AnalysisException {
        var dataset = buildDataset(parameters, createTableOrViewPreference);

        return writeDataset(dataset, parameters);

    }

    public abstract StreamingQuery writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) throws TimeoutException;
}
