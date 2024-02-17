package io.openenterprise.daisy.test.spark.sql.streaming;

import com.google.common.collect.Lists;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;

@TestConfiguration
@ComponentScan("io.openenterprise.daisy.spark.sql.streaming")
public class Configuration {

    private static final String KAFKA_DOCKER_IMAGE_TAG = "confluentinc/cp-kafka:7.5.3";

    @Bean
    protected KafkaContainer kafkaContainer(@Nonnull Network network) {
        var dockerImageName = DockerImageName.parse(KAFKA_DOCKER_IMAGE_TAG);

        var kafkaContainer = new KafkaContainer(dockerImageName)
                .withExposedPorts(9092, 9093)
                .withKraft()
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withNetworkMode("host");

        kafkaContainer.setPortBindings(Lists.newArrayList("9092:9092", "9093:9093"));
        kafkaContainer.start();

        try {
            return kafkaContainer;
        } finally {
            Testcontainers.exposeHostPorts(9092);
        }
    }

}
