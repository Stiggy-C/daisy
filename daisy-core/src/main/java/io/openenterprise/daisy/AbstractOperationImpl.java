package io.openenterprise.daisy;

import com.google.common.collect.Sets;
import io.openenterprise.Parameter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.cache.Cache;
import javax.inject.Inject;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

import static io.openenterprise.daisy.Parameter.SESSION_ID;

public abstract class AbstractOperationImpl<T> implements Operation<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(Operation.class);

    protected static final Set<? extends Parameter> REQUIRED_PARAMETERS = Sets.newHashSet(SESSION_ID);

    @Inject
    protected Cache<UUID, InvocationContext> invocationContextCache;

    @Nonnull
    protected Invocation<T> createInvocation(@Nonnull InvocationContext invocationContext,
                                             @Nonnull Map<String, Object> parameters) {
        return initInvocation(new Invocation<>(), invocationContext, parameters);
    }

    @Nullable
    protected InvocationContext getInvocationContext(@Nonnull UUID id) {
        return invocationContextCache.get(id);
    }

    @Nonnull
    protected InvocationContext getOrCreateInvocationContext(@Nonnull UUID id) {
        if (invocationContextCache.containsKey(id)) {
            var invocationContext = getInvocationContext(id);

            assert Objects.nonNull(invocationContext);

            return invocationContext;
        }

        var innovationContext = new InvocationContext(id);

        try {
            return innovationContext;
        } finally {
            invocationContextCache.put(id, innovationContext);
        }
    }

    @Nonnull
    protected UUID getSessionId(@Nonnull Map<String, Object> parameters) {
        var object = MapUtils.getObject(parameters, SESSION_ID.getKey());

        UUID uuid;

        if (ClassUtils.isAssignable(object.getClass(), String.class)) {
            uuid = UUID.fromString(object.toString());
        } else if (ClassUtils.isAssignable(object.getClass(), UUID.class)) {
            uuid = (UUID) object;
        } else {
            throw new UnsupportedOperationException(SESSION_ID.getKey()
                    + " must be a String representation of an UUID or of type, java.util.UUID");
        }

        return uuid;
    }

    @Nonnull
    protected UUID getOrCreateSessionId(@Nonnull Map<String, Object> parameters) {
        var hasSessionId = parameters.containsKey(SESSION_ID.getKey());
        var sessionId = hasSessionId? getSessionId(parameters) : UUID.randomUUID();

        parameters.putIfAbsent(SESSION_ID.getKey(), sessionId);

        return sessionId;
    }

    @Nonnull
    protected Invocation<T> init(@Nonnull Map<String, Object> parameters) {
        var invocationContextId = getOrCreateSessionId(parameters);
        var invocationContext = getOrCreateInvocationContext(invocationContextId);

        return createInvocation(invocationContext, parameters);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    protected Invocation<T> initInvocation(@Nonnull Invocation<T> invocation, @Nonnull InvocationContext invocationContext,
                                           @Nonnull Map<String, Object> parameters) {
        invocation.setOperationClass((Class<? extends Operation<T>>) this.getClass());
        invocation.setParameters(parameters);

        // Add the previous currentInvocation to io.openenterprise.daisy.InvocationContext.pastInvocations
        Optional.ofNullable(invocationContext.getPastInvocations())
                .ifPresent(pastInvocations -> pastInvocations.add(invocation));

        invocationContext.setCurrentInvocation(invocation);

        return invocation;
    }

    @Nonnull
    protected abstract Map<String, Object> manipulateParameters(@Nonnull Map<String, Object> parameters);

    protected void verifyParameters(@Nonnull Map<String, Object> parameters) {
        verifyParameters(parameters, REQUIRED_PARAMETERS);
    }

    protected void verifyParameters(@Nonnull Map<String, Object> parameters, @Nonnull Set<? extends Parameter> requiredParameters) {
        requiredParameters.stream()
                .filter(param -> !parameters.containsKey(param.getKey())
                        && Objects.isNull(MapUtils.getObject(parameters, param.getKey())))
                .findFirst()
                .ifPresent(param -> {
                    throw new IllegalArgumentException(param.getKey() + " is not present");
                });

        requiredParameters.stream()
                .filter(param -> !ClassUtils.isAssignable(MapUtils.getObject(parameters, param.getKey()).getClass(),
                        param.getValueType()))
                .findFirst()
                .ifPresent(param -> {
                    throw new IllegalArgumentException(param.getKey() + " is not of type, " + param.getValueType());
                });
    }

    @Nullable
    protected T withInvocationContext(@Nonnull Map<String, Object> parameters,
                                      @Nonnull Function<Map<String, Object>, T> function) {
        var invocation = init(parameters);

        T result = null;

        try {
            invocation.setInvocationInstant(Instant.now());
            verifyParameters(parameters);

            result = function.apply(manipulateParameters(parameters));
        } catch (Exception e) {
            invocation.setThrowable(e);

            LOG.error("Un-expected exception occurred when invoking " + this.getClass(), e);

            throw e;
        } finally {
            invocation.setCompletionInstant(Instant.now());
            invocation.setResult(result);
        }

        return result;
    }
}
