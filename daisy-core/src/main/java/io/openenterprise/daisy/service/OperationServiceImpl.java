package io.openenterprise.daisy.service;

import io.openenterprise.daisy.Operation;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Map;
import java.util.Objects;

@Service
public class OperationServiceImpl implements OperationService {

    protected static final Logger LOG = LoggerFactory.getLogger(OperationService.class);

    @Inject
    protected ApplicationContext applicationContext;

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invoke(@Nonnull String beanNameOrClassName, @Nonnull Map<String, Object> parameters) {
        var clazz = getDatasetOperationClass(beanNameOrClassName);

        if (Objects.isNull(clazz)) {
            throw new IllegalArgumentException("No such " + beanNameOrClassName);
        }

        var bean = getDatasetOperation(clazz, beanNameOrClassName);

        return (T) invoke(bean, parameters);
    }

    @Override
    public <T> T invoke(@Nonnull Operation<T> operation, @Nonnull Map<String, Object> parameters) {
        return operation.invoke(parameters);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    protected <T extends Operation<?>> Class<T> getDatasetOperationClass(@Nonnull String beanNameOrClassName) {
        Class<T> clazz;

        try {
            clazz = applicationContext.containsBean(beanNameOrClassName)?
                    (Class<T>) applicationContext.getBean(beanNameOrClassName, Operation.class).getClass() :
                    (Class<T>) ClassUtils.getClass(beanNameOrClassName);
        } catch (BeansException e) {
            LOG.error("Un-expected exception when getting bean of requested type", e);

            throw e;
        } catch (ClassNotFoundException e) {
            LOG.error("No such class, {}", beanNameOrClassName);

            clazz = null;
        }

        return clazz;
    }

    @Nonnull
    protected <T extends Operation<?>> T getDatasetOperation(@Nonnull Class<T> clazz, @Nullable String beanName) {
        return StringUtils.isEmpty(beanName)? applicationContext.getBean(clazz) :
                applicationContext.containsBean(beanName)? applicationContext.getBean(beanName, clazz) :
                        applicationContext.getBean(clazz);
    }
}
