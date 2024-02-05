package io.openenterprise.daisy.example.spark;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Maps;
import io.openenterprise.daisy.Parameter;
import io.openenterprise.daisy.AbstractMvelDrivenOperationImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.Map;

@Component("mvelDrivenRecentPurchaseExample")
@ConditionalOnBean(AmazonS3.class)
@Profile("pipeline_example")
public class RecentPurchaseExampleMvelDrivenOperation extends AbstractMvelDrivenOperationImpl<Void> {

    public RecentPurchaseExampleMvelDrivenOperation() {
        super(Void.class);
    }

    @Value("${recentPurchaseExampleMvelPipeline.build-dataset-expressions}")
    protected String buildDatasetExpressions;

    @Value("${recentPurchaseExampleMvelPipeline.plot-expressions}")
    protected String plotExpressions;

    @Value("${recentPurchaseExampleMvelPipeline.to-plot-json-expressions}")
    protected String toPlotJsonExpressions;

    @Value("${recentPurchaseExampleMvelPipeline.write-dataset-expressions}")
    protected String writeDatasetExpressions;

    public Void invoke(@Nonnull Map<String, Object> parameters) {
        parameters = Maps.newHashMap(parameters);
        parameters.put(Parameter.MVEL_EXPRESSIONS.getKey(), buildDatasetExpressions + writeDatasetExpressions);

        return super.invoke(parameters);
    }
}

