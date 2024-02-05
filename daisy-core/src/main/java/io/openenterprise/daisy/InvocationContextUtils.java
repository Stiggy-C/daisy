package io.openenterprise.daisy;

import lombok.Getter;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import javax.inject.Inject;
import java.util.Objects;
import java.util.UUID;

@Component
public final class InvocationContextUtils {

    private static volatile Cache<UUID, InvocationContext> INVOCATION_CONTEXT_CACHE;

    public static Cache<UUID, InvocationContext> getInvocationContextCache() {
        return INVOCATION_CONTEXT_CACHE;
    }

    @Inject
    public void setInvocationContextCache(@Nonnull Cache<UUID, InvocationContext> invocationContextCache) {
        synchronized (InvocationContextUtils.class) {
            if (Objects.isNull(INVOCATION_CONTEXT_CACHE)) {
                INVOCATION_CONTEXT_CACHE = invocationContextCache;
            }
        }
    }
}
