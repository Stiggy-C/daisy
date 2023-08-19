package io.openenterprise.daisy.examples;

import com.amazonaws.services.s3.AmazonS3;
import io.openenterprise.daisy.spark.sql.AbstractMvelPlotGeneratingDatasetComponentImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

@Component("recentPurchaseExampleMvelPipeline")
@ConditionalOnBean(AmazonS3.class)
@Profile("pipeline_example")
public class RecentPurchaseExampleMvelDatasetComponent extends AbstractMvelPlotGeneratingDatasetComponentImpl {

    @Value("#{'${recentPurchaseExampleMvelPipeline.build-dataset-expressions}'.split(';')}")
    public void setBuildDatasetExpressions(@Nonnull String[] buildDatasetExpressions) {
        super.buildDatasetExpressions = buildDatasetExpressions;
    }

    @Value("#{'${recentPurchaseExampleMvelPipeline.pipeline-expressions}'.split(';')}")
    public void setPipelineExpressions(@Nonnull String[] pipelineExpressions) {
        super.pipelineExpressions = pipelineExpressions;
    }

    @Value("#{'${recentPurchaseExampleMvelPipeline.plot-expressions}'.split(';')}")
    public void setPlotExpressions(@Nonnull String[] plotExpressions) {
        super.plotExpressions = plotExpressions;
    }

    @Value("#{'${recentPurchaseExampleMvelPipeline.to-plot-json-expressions}'.split(';')}")
    public void setToPlotJsonExpressions(@Nonnull String[] toPlotJsonExpressions) {
        super.toPlotJsonExpressions = toPlotJsonExpressions;
    }

    @Value("#{'${recentPurchaseExampleMvelPipeline.write-dataset-expressions}'.split(';')}")
    public void setWriteDatasetExpressions(@Nonnull String[] writeDatasetExpressions) {
        super.writeDatasetExpressions = writeDatasetExpressions;
    }
}

