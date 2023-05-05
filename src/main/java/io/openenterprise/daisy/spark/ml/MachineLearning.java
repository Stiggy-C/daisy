package io.openenterprise.daisy.spark.ml;

import org.apache.spark.ml.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;

public interface MachineLearning<T extends Transformer> {

    /**
     * Make 1 or more predictions by making use of given model. Given input json can be one row or multiple rows of data.
     * However, they must have the same schema with the dataset which used to build the given model. Need to be filled
     * in by the implementation.
     *
     * @param model
     * @param jsonString
     * @return
     */
    @Nonnull
    Dataset<Row> predict(@Nonnull T model, @Nonnull String jsonString);

}