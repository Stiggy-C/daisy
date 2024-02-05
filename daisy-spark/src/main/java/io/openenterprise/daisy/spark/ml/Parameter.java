package io.openenterprise.daisy.spark.ml;

import lombok.Getter;
import org.apache.spark.ml.Model;

import javax.annotation.Nonnull;

public enum Parameter implements io.openenterprise.Parameter {

    ML_MODEL("daisy.spark.ml.model", Model.class),

    ML_MODEL_ID("daisy.spark.ml.model-id", String.class),

    ML_SAVE_MODEL("daisy.spark.ml.save-model", Boolean.class);

    @Getter
    private final String key;

    @Getter
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
