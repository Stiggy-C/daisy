package io.openenterprise.daisy.test.spark.ml;

import io.openenterprise.daisy.spark.ml.ModelStorage;
import io.openenterprise.daisy.spark.ml.amazonaws.AmazonS3ModelStorage;
import io.openenterprise.daisy.spark.ml.clustering.KMeansPredictOperation;
import io.openenterprise.daisy.spark.ml.clustering.KMeansTrainOperation;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@TestConfiguration
@ComponentScan("io.openenterprise.daisy.spark.ml")
public class Configuration extends io.openenterprise.daisy.test.spark.Configuration {

    @Bean
    protected ModelStorage amazonS3ModelStorage() {
        return new AmazonS3ModelStorage();
    }

}
