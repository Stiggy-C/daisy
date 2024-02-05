package io.openenterprise.daisy.springframework.context.support;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Objects;

@Component
public final class ApplicationContextUtils  {

    private static volatile ApplicationContextContainer APPLICATION_CONTEXT_CONTAINER;

    public static ApplicationContext getApplicationContext() {
        return APPLICATION_CONTEXT_CONTAINER.applicationContext;
    }

    @Inject
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) {
        synchronized (ApplicationContextContainer.class) {
            if (Objects.isNull(APPLICATION_CONTEXT_CONTAINER)) {
                APPLICATION_CONTEXT_CONTAINER = new ApplicationContextContainer(applicationContext);
            }
        }
    }

    private static class ApplicationContextContainer {

        private final ApplicationContext applicationContext;

        public ApplicationContextContainer(ApplicationContext applicationContext) {
            this.applicationContext = applicationContext;
        }
    }
}
