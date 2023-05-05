package io.openenterprise.daisy.spark.ml;


import org.apache.spark.ml.Model;
import org.apache.spark.ml.util.MLWritable;

import javax.annotation.Nonnull;
import java.net.URI;

public interface ModelStorage {

    URI getUriOfModel(@Nonnull String uid);

    <M extends Model<M> & MLWritable> M load(@Nonnull Class<M> modelClass, @Nonnull String uid);

    <M extends Model<M> & MLWritable> URI store(@Nonnull M model);
}
