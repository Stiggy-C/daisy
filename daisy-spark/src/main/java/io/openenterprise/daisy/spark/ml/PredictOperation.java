package io.openenterprise.daisy.spark.ml;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.Map;

public interface PredictOperation<M extends Transformer & MLWritable> extends MLOperation<M, Dataset<Row>> {

    @Nonnull
    Dataset<Row> predict(@Nonnull Dataset<Row> dataset, @Nonnull M model, @Nonnull Map<String, Object> parameters);
}

