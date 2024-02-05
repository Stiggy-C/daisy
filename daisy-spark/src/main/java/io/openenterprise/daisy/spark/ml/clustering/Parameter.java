package io.openenterprise.daisy.spark.ml.clustering;

import lombok.Getter;

import javax.annotation.Nonnull;

@Getter
public enum Parameter implements io.openenterprise.Parameter {

    FEATURES_COLUMN("daisy.spark.ml.cluster.feature-columns", String[].class),

    KMEANS_K("daisy.spark.ml.clustering.k", Integer.class),

    KMEANS_MAX_ITER("daisy.spark.ml.clustering.max-iter", Integer.class);

    private final String key;

    private final Class<?> valueType;

    @Nonnull
    @SuppressWarnings("unchecked")
    public  <T> Class<T> getValueType() {
        return (Class<T>) valueType;
    }

    Parameter(String key, Class<?> valueType) {
        this.key = key;
        this.valueType = valueType;
    }
}
