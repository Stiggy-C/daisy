package io.openenterprise.daisy.spark.ml;

import io.openenterprise.daisy.Operation;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.util.MLWritable;

public interface MLOperation<M extends Transformer & MLWritable, T> extends Operation<T> {
}
