package io.openenterprise.daisy.spark;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.Lists;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.spark.sql.SparkSession;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.util.concurrent.Executors;

@TestConfiguration
public class BaseTestConfiguration {

    private static final String LOCALSTACK_DOCKER_IMAGE_TAG = "localstack/localstack:1.4.0";

    private static final String MYSQL_DOCKER_IMAGE_TAG = "mysql:8.0.32";

    private static final String POSTGRESQL_DOCKER_IMAGE_TAG = "postgres:15.2";

    private static final String SPARK_DOCKER_IMAGE_TAG = "daisy/spark:3.3.2";

    @Bean
    protected AmazonS3 amazonS3(LocalStackContainer localStackContainer) {
        return AmazonS3ClientBuilder.standard()
                .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "host.testcontainers.internal:4566", localStackContainer.getRegion()))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    @Bean
    protected LocalStackContainer localStackContainer(@Nonnull Network network) {
        var dockerImageName = DockerImageName.parse(LOCALSTACK_DOCKER_IMAGE_TAG);
        var localStackContainer = new LocalStackContainer(dockerImageName)
                .withExposedPorts(4566)
                .withNetwork(network)
                .withNetworkAliases("aws")
                .withNetworkMode("host")
                .withServices(LocalStackContainer.Service.S3);

        localStackContainer.setPortBindings(Lists.newArrayList("4566:4566"));

        localStackContainer.start();

        try {
            return localStackContainer;
        } finally {
            Testcontainers.exposeHostPorts(4566);
        }
    }

    @Bean
    @Qualifier("mysqlDataSource")
    protected DataSource mysqlDataSource(@Nonnull MySQLContainer mySQLContainer) {
        var datasource = new MysqlDataSource();
        datasource.setUrl(mySQLContainer.getJdbcUrl());
        datasource.setUser(mySQLContainer.getUsername());
        datasource.setPassword(mySQLContainer.getPassword());
        datasource.setDatabaseName(mySQLContainer.getDatabaseName());

        return datasource;
    }

    @Bean
    protected <T extends MySQLContainer<T>> MySQLContainer<T> mySQLContainer(@Nonnull Network network) {
        var dockerImageName = DockerImageName.parse(MYSQL_DOCKER_IMAGE_TAG);
        var mySqlContainer = new MySQLContainer<T>(dockerImageName)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withMemory(512 * 1024 * 1024L))
                .withExposedPorts(3306)
                .withNetwork(network)
                .withNetworkAliases("mysql")
                .withNetworkMode("host");

        mySqlContainer.setPortBindings(Lists.newArrayList("3306:3306"));
        mySqlContainer.start();

        try {
            return mySqlContainer;
        } finally {
            Testcontainers.exposeHostPorts(3306);
        }
    }

    @Bean
    protected Network network() {
        return Network.newNetwork();
    }

    @Bean
    protected <T extends PostgreSQLContainer<T>> PostgreSQLContainer<T> postgreSQLContainer(@Nonnull Network network) {
        var dockerImageName = DockerImageName.parse(POSTGRESQL_DOCKER_IMAGE_TAG);
        var postgresqlContainer = new PostgreSQLContainer<T>(dockerImageName)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withMemory(512 * 1024 * 1024L))
                .withExposedPorts(5432)
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withNetworkMode("host");

        postgresqlContainer.setPortBindings(Lists.newArrayList("5432:5432"));
        postgresqlContainer.start();

        try {
            return postgresqlContainer;
        } finally {
            Testcontainers.exposeHostPorts(5432);
        }
    }

    @Bean
    @Qualifier("postgresDatasource")
    protected DataSource postgresDatasource(@Nonnull PostgreSQLContainer postgreSQLContainer) {
        var datasource = new PGSimpleDataSource();
        datasource.setUrl(postgreSQLContainer.getJdbcUrl());
        datasource.setUser(postgreSQLContainer.getUsername());
        datasource.setPassword(postgreSQLContainer.getPassword());
        datasource.setDatabaseName(postgreSQLContainer.getDatabaseName());

        return datasource;
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @Profile("!local-spark")
    @Qualifier("sparkMasterContainer")
    protected GenericContainer<?> sparkMasterContainer(@Nonnull Network network) {
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

        return genericContainer;
    }

    @Bean
    @Order
    protected SparkSession sparkSessionLocal(LocalStackContainer localStackContainer) {
        return SparkSession.builder()
                .appName("daisy-unit-test")
                .master("local[*]")
                .config("spark.hadoop.fs.s3a.access.key", localStackContainer.getAccessKey())
                .config("spark.hadoop.fs.s3a.secret.key", localStackContainer.getSecretKey())
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", false)
                .config("spark.hadoop.fs.s3a.endpoint", "127.0.0.1:4566")
                .config("spark.hadoop.fs.s3a.path.style.access", true)
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.submit.deployMode", "client")
                .getOrCreate();
    }

    @Bean
    @Order
    @Profile("!local-spark")
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
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.submit.deployMode", "client")
                .getOrCreate();
    }

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @Profile("!local-spark")
    @Qualifier("sparkWorkerContainer")
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
