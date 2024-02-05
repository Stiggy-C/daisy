package io.openenterprise.daisy;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Getter
public class Invocation<T> {

    protected final UUID id = UUID.randomUUID();

    @Setter
    protected Instant completionInstant;

    @Setter
    protected Class<? extends Operation<T>> operationClass;

    @Setter
    protected Map<String, Object> parameters;

    @Setter
    protected Instant invocationInstant;

    @Setter
    protected T result;

    @Setter
    protected Throwable throwable;
}
