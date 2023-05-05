package io.openenterprise.daisy.rs;

import io.openenterprise.daisy.rs.model.TriggerPipelineResponse;
import io.openenterprise.daisy.spark.AbstractPipeline;
import io.openenterprise.daisy.spark.AbstractStreamingPipeline;
import lombok.SneakyThrows;
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
        var bean = applicationContext.getBean(name);
        TriggerPipelineResponse triggerPipelineResponse;

        if (isAssignable(bean.getClass(), AbstractPipeline.class) || isAssignable(bean.getClass(), AbstractStreamingPipeline.class)) {
            triggerPipelineResponse = isAssignable(bean.getClass(), AbstractPipeline.class) ?
                    runPipeline((AbstractPipeline) bean, parameters) :
                    startStreamingPipeline((AbstractStreamingPipeline) bean, parameters);
        } else {
            throw new UnsupportedOperationException();
        }

        return triggerPipelineResponse;
    }

    @Nonnull
    protected TriggerPipelineResponse runPipeline(
            @Nonnull AbstractPipeline pipeline, @Nonnull Map<String, Object> parameters) {
        pipeline.run(parameters);

        return new TriggerPipelineResponse().isStreaming(false);
    }

    @Nonnull
    protected TriggerPipelineResponse startStreamingPipeline(
            @Nonnull AbstractStreamingPipeline streamingPipeline, @Nonnull Map<String, Object> parameters) throws TimeoutException {
        var streamingQuery = streamingPipeline.start(parameters);

        return new TriggerPipelineResponse().isStreaming(true).streamingQueryId(streamingQuery.id());
    }
}
