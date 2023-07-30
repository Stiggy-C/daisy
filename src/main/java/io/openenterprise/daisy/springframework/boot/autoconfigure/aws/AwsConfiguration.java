package io.openenterprise.daisy.springframework.boot.autoconfigure.aws;

import io.openenterprise.daisy.spark.ml.ModelStorage;
import io.openenterprise.daisy.spark.ml.amazonaws.AmazonS3ModelStorage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("aws")
public class AwsConfiguration {

    @Bean
    protected ModelStorage amazonS3ModelStorage() {
        return new AmazonS3ModelStorage();
    }
}
