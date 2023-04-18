package io.openenterprise.daisy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.Map;

public abstract class AbstractPipeline extends AbstractApplication {

    public void run(@Nonnull Map<String, ?> parameters) {
        Dataset<Row> dataset = buildDataset(parameters);
        writeDataset(dataset, parameters);
    }

    protected abstract void writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);
}
