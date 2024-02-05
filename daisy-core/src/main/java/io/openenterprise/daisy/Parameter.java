package io.openenterprise.daisy;

import lombok.Getter;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

@Getter
public enum Parameter implements io.openenterprise.Parameter {

    DATASET_OPERATIONS("daisy.dataset-operations", List.class),

    MVEL_CLASS_IMPORTS("daisy.mvel.class-imports", String[].class),

    MVEL_EXPRESSIONS("daisy.mvel.expressions", String.class),

    MVEL_PACKAGE_IMPORTS("daisy.mvel.package-imports", String[].class),

    PARAMETERS("daisy.parameters", List.class),

    SESSION_ID("daisy.session.id", UUID.class);

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