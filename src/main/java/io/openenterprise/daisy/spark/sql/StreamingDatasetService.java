package io.openenterprise.daisy.spark.sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface StreamingDatasetService extends BaseDatasetService {

    /**
     * Run this as a streaming pipeline
     *
     * @param parameters
     * @return
     * @throws TimeoutException
     * @throws AnalysisException
     */
    StreamingQuery streamingPipeline(@Nonnull Map<String, ?> parameters) throws TimeoutException, AnalysisException;

    /**
     * Stream the data of the (aggregated) dataset to desired data source. Need to be filled in by the implementation.
     *
     * @param dataset
     * @param parameters
     * @return
     * @throws TimeoutException
     */
    StreamingQuery writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) throws TimeoutException;
}

interface MvelStreamingDatasetService extends StreamingDatasetService {}