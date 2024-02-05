package io.openenterprise.daisy.spark.ml;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.Map;

public interface TrainOperation<M extends Transformer & MLWritable> extends MLOperation<M, M> {

    @Nonnull
    M train(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters);

}
