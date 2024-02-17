package io.openenterprise.daisy.spark.sql.streaming;

import lombok.Getter;

import javax.annotation.Nonnull;

@Getter
public enum Parameter implements io.openenterprise.Parameter {

    CHECKPOINT_LOCATION("daisy.spark.dataset.streaming.checkpoint-location", String.class),

    KAFKA_ASSIGN("daisy.spark.dataset.streaming.kafka.assign", String.class),

    KAFKA_BOOT_STRAP_SERVERS("kafka.bootstrap.servers", String.class),

    KAFKA_SUBSCRIBE("daisy.spark.dataset.streaming.kafka.subscribe", String.class),

    KAFKA_SUBSCRIBE_PATTERN("daisy.spark.dataset.streaming.kafka.subscribe-pattern", String.class),

    OUTPUT_MODE("daisy.spark.dataset.streaming.output-mode", String.class);

    private final String key;

    private final Class<?> valueType;

    Parameter(String key, Class<?> valueType) {
        this.key = key;
        this.valueType = valueType;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public  <T> Class<T> getValueType() {
        return (Class<T>) valueType;
    }
}
