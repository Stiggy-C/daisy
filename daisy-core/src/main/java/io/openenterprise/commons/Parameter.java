package io.openenterprise.commons;

import lombok.Getter;

import javax.annotation.Nonnull;

@Getter
public enum Parameter implements io.openenterprise.Parameter {

    JSON("json", String.class);

    private final String key;

    private final Class<?> valueType;

    @Nonnull
    @SuppressWarnings("unchecked")
    public  <T> Class<T> getValueType() {
        return (Class<T>) valueType;
    }

    Parameter(String name, Class<?> valueType) {
        this.key = name;
        this.valueType = valueType;
    }
}
