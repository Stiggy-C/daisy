package io.openenterprise.daisy;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.openenterprise.daisy.Parameter.*;

/**
 * An implementation of {@link Operation} to run a {@link List} of {@link Operation} in the given sequence
 *
 * @see Operation
 */
/*
    DO NOT convert to Kotlin!!!
 */
@Component
public class CompositeOperation extends AbstractOperationImpl<Void> implements Operation<Void> {

    protected static final Logger LOG = LoggerFactory.getLogger(CompositeOperation.class);

    protected final static Set<Parameter> REQUIRED_PARAMETERS = Sets.newHashSet(DATASET_OPERATIONS,
            PARAMETERS);

    @Inject
    protected ApplicationContext applicationContext;

    @Nonnull
    @Override
    protected Map<String, Object> manipulateParameters(@Nonnull Map<String, Object> parameters) {
        return parameters;
    }

    @Override
    protected void verifyParameters(@NotNull Map<String, Object> parameters) {
        super.verifyParameters(parameters);
        verifyParameters(parameters, REQUIRED_PARAMETERS);
    }

    @Nullable
    @Override
    public Void invoke(@Nonnull Map<String, Object> parameters) {
        return withInvocationContext(parameters, (params) ->  {
            @SuppressWarnings("unchecked")
            var datasetOperationBeanNameOrClasses = (List<String>) MapUtils.getObject(parameters,
                    DATASET_OPERATIONS.getKey());
            var datasetOperations = getDatasetOperations(datasetOperationBeanNameOrClasses);
            @SuppressWarnings("unchecked")
            var parametersList = (List<Map<String, Object>>) MapUtils.getObject(parameters, PARAMETERS.getKey());

            for (int i = 0; i < datasetOperations.size(); i++) {
                var thisDatasetOperation = datasetOperations.get(i);
                var thisParameters = parametersList.get(i);

                thisParameters.put(SESSION_ID.getKey(), getSessionId(params));

                LOG.info("Invoking operation, {}", thisDatasetOperation);

                thisDatasetOperation.invoke(thisParameters);

                LOG.info("Invoked operation, {}", thisDatasetOperation);
            }

            return null;
        });
    }

    @Nonnull
    private List<Operation<?>> getDatasetOperations(@Nonnull List<String> datasetOperationBeanNamesOrClasses) {
        var datasetOperationsMap = Maps.<String, Operation<?>>newHashMap();

        // Handle bean Id's/names first
        datasetOperationBeanNamesOrClasses.stream()
                .filter(applicationContext::containsBean)
                .forEach(beanName -> datasetOperationsMap.put(beanName,
                        applicationContext.getBean(beanName, Operation.class)));

        // Handle left over which should be the FQDN of classes
        var classes = datasetOperationBeanNamesOrClasses.stream()
                .filter(string -> !datasetOperationBeanNamesOrClasses.contains(string)).collect(Collectors.toList());

        ClassUtils.convertClassNamesToClasses(classes)
                .forEach(clazz ->  datasetOperationsMap.put(clazz.getName(),
                        (Operation<?>) applicationContext.getBean(clazz)));

        return datasetOperationBeanNamesOrClasses.stream().map(datasetOperationsMap::get).collect(Collectors.toList());
    }
}
