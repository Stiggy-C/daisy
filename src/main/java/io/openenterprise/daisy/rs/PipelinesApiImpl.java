package io.openenterprise.daisy.rs;

import io.openenterprise.daisy.rs.PipelinesApi;
import io.openenterprise.daisy.rs.model.TriggerPipelineResponse;
import io.openenterprise.daisy.spark.sql.AbstractDatasetService;
import io.openenterprise.daisy.spark.sql.AbstractStreamingDatasetService;
import lombok.SneakyThrows;
import org.apache.spark.sql.AnalysisException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.ClassUtils.isAssignable;

@Component
public class PipelinesApiImpl implements PipelinesApi {

    @Inject
    protected ApplicationContext applicationContext;

    @SneakyThrows
    @Override
    public TriggerPipelineResponse triggerPipeline(@Nonnull Map<String, Object> parameters, @Nonnull String name) {
        if (!applicationContext.containsBean(name)) {
            throw new NoSuchBeanDefinitionException(name);
        }

        var bean = applicationContext.getBean(name);
        TriggerPipelineResponse triggerPipelineResponse;

        if (isAssignable(bean.getClass(), AbstractDatasetService.class) || isAssignable(bean.getClass(), AbstractStreamingDatasetService.class)) {
            triggerPipelineResponse = isAssignable(bean.getClass(), AbstractDatasetService.class) ?
                    runPipeline((AbstractDatasetService) bean, parameters) :
                    startStreamingPipeline((AbstractStreamingDatasetService) bean, parameters);
        } else {
            throw new UnsupportedOperationException();
        }

        return triggerPipelineResponse;
    }

    @Nonnull
    protected TriggerPipelineResponse runPipeline(
            @Nonnull AbstractDatasetService datasetService, @Nonnull Map<String, Object> parameters) throws AnalysisException {
        datasetService.pipeline(parameters);

        return new TriggerPipelineResponse().isStreaming(false);
    }

    @Nonnull
    protected TriggerPipelineResponse startStreamingPipeline(
            @Nonnull AbstractStreamingDatasetService streamingDatasetService, @Nonnull Map<String, Object> parameters)
            throws TimeoutException, AnalysisException {
        var streamingQuery = streamingDatasetService.streamingPipeline(parameters);

        return new TriggerPipelineResponse().isStreaming(true).streamingQueryId(streamingQuery.id());
    }
}
