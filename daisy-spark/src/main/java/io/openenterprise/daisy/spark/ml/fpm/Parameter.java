package io.openenterprise.daisy.spark.ml.fpm;

import lombok.Getter;

import javax.annotation.Nonnull;

@Getter
public enum Parameter implements io.openenterprise.Parameter {

    MIN_CONFIDENCE("daisy.spark.ml.fpm.min-confidence", Double.class),

    MIN_SUPPORT("daisy.spark.ml.fpm.min-support", Double.class);

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
