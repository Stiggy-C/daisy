package io.openenterprise.daisy.spark.ml;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.util.MLWritable;

import javax.annotation.Nonnull;
import java.net.URI;

public interface ModelStorage {

    URI getUriOfModel(@Nonnull String uid);

    <M extends Transformer & MLWritable> M load(@Nonnull Class<M> modelClass, @Nonnull String uid);

    <M extends Transformer & MLWritable> URI store(@Nonnull M model);
}
