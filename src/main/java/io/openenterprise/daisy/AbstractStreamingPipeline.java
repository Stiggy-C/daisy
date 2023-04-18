package io.openenterprise.daisy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public abstract class AbstractStreamingPipeline extends AbstractApplication {

    public StreamingQuery run(@Nonnull Map<String, ?> parameters) throws TimeoutException {
        Dataset<Row> dataset = buildDataset(parameters);

        return writeDataset(dataset, parameters);
    }

    protected abstract StreamingQuery writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) throws TimeoutException;
}
