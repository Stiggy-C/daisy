package io.openenterprise.daisy.test.spark;

import com.google.common.collect.Lists;
import io.openenterprise.daisy.spark.sql.SimpleLoadDatasetOperation;
import io.openenterprise.daisy.spark.sql.SimpleSaveDatasetOperation;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;
import java.io.IOException;

@TestConfiguration
@ComponentScan("io.openenterprise.daisy.spark.sql")
public class Configuration extends io.openenterprise.daisy.test.Configuration {

    private static final String SPARK_DOCKER_IMAGE_TAG = "daisy/spark:3.5.0";

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @Profile("!local_spark")
    @Qualifier("sparkMasterContainer")
    protected GenericContainer<?> sparkMasterContainer(@Nonnull Network network) throws IOException, InterruptedException {
        var dockerImageName = DockerImageName.parse(SPARK_DOCKER_IMAGE_TAG);
        var genericContainer = new GenericContainer<>(dockerImageName)
                .waitingFor(Wait.forLogMessage(".*Master: I have been elected leader! New state: ALIVE.*", 1))
                .withCommand("/opt/bitnami/spark/sbin/start-master.sh")
                .withEnv("SPARK_MASTER_HOST", "0.0.0.0")
                .withExposedPorts(4044, 7077, 8080)
                .withNetwork(network)
                .withNetworkAliases("spark-master")
                .withNetworkMode("host");
        genericContainer.setPortBindings(Lists.newArrayList("4044:4044", "7077:7077", "8080:8080"));

        genericContainer.start();
        // genericContainer.execInContainer("/opt/bitnami/spark/sbin/start-thriftserver.sh");

        return genericContainer;
    }

    @Bean
    @Order
    @Profile("local_spark")
    protected SparkSession sparkSessionLocal(LocalStackContainer localStackContainer) {
        return SparkSession.builder()
                .appName("daisy-unit-test")
                .master("local[*]")
                .config("spark.hadoop.fs.s3a.access.key", localStackContainer.getAccessKey())
                .config("spark.hadoop.fs.s3a.secret.key", localStackContainer.getSecretKey())
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", false)
                .config("spark.hadoop.fs.s3a.endpoint", "127.0.0.1:4566")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.submit.deployMode", "client")
                .getOrCreate();
    }

    @Bean
    @DependsOn("sparkWorkerContainer")
    @Order
    @Profile("!local_spark")
    protected SparkSession sparkSessionRemote(LocalStackContainer localStackContainer) {
        return SparkSession.builder()
                .appName("daisy-unit-test")
                .master("spark://127.0.0.1:7077")
                .config("spark.executor.memory", "512m")
                .config("spark.executor.memoryOverhead", "512m")
                .config("spark.executor.heartbeatInterval", "20s")
                .config("spark.hadoop.fs.s3a.access.key", localStackContainer.getAccessKey())
                .config("spark.hadoop.fs.s3a.secret.key", localStackContainer.getSecretKey())
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", false)
                .config("spark.hadoop.fs.s3a.endpoint", "host.testcontainers.internal:4566")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                .config("spark.memory.offHeap.enabled", "true")
                .config("spark.memory.offHeap.size", "512m")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.submit.deployMode", "client")
                .getOrCreate();
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @Profile("!local_spark")
    protected GenericContainer<?> sparkWorkerContainer(
            @Nonnull Network network, @Qualifier("sparkMasterContainer") GenericContainer<?> sparkMasterContainer) {
        var sparkMasterContainerIpAddress = sparkMasterContainer.getContainerInfo().getNetworkSettings()
                .getIpAddress();

        var dockerImageName = DockerImageName.parse(SPARK_DOCKER_IMAGE_TAG);
        var genericContainer = new GenericContainer<>(dockerImageName)
                .waitingFor(Wait.forLogMessage(".*Worker: Successfully registered with master.*", 1))
                .withAccessToHost(true)
                .withCommand("/opt/bitnami/spark/sbin/start-worker.sh spark://" + sparkMasterContainerIpAddress + ":7077")
                .withExposedPorts(8081)
                .withNetwork(network)
                .withNetworkAliases("spark-worker")
                .withNetworkMode("host");
        genericContainer.setPortBindings(Lists.newArrayList("8081:8081"));

        genericContainer.start();

        return genericContainer;
    }

}
