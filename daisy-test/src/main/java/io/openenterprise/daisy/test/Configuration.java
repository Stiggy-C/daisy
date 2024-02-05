package io.openenterprise.daisy.test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

@TestConfiguration
@ComponentScan({"io.openenterprise.daisy.service", "io.openenterprise.daisy.springframework.context.support"})
public class Configuration {

    private static final String LOCALSTACK_DOCKER_IMAGE_TAG = "localstack/localstack:3.0.2";

    private static final String MYSQL_DOCKER_IMAGE_TAG = "mysql:8.0.35";

    private static final String POSTGRESQL_DOCKER_IMAGE_TAG = "postgres:15.5";

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
                .withAccessToHost(true)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withMemory(512 * 1024 * 1024L))
                .withExposedPorts(3306)
                .withNetwork(network)
                .withNetworkAliases("mysql")
                .withNetworkMode("host");

        mySqlContainer.setPortBindings(Lists.newArrayList("3306:3306"));
        mySqlContainer.start();

        Testcontainers.exposeHostPorts(3306);

        return mySqlContainer;
    }

    @Bean
    protected MemberDataGenerator memberDataGenerator() {
        return new MemberDataGenerator();
    }

    @Bean
    protected Network network() {
        return Network.newNetwork();
    }

    @Bean
    protected ObjectMapper objectMapper() {
        return new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    protected <T extends PostgreSQLContainer<T>> PostgreSQLContainer<T> postgreSQLContainer(@Nonnull Network network) {
        var dockerImageName = DockerImageName.parse(POSTGRESQL_DOCKER_IMAGE_TAG);
        var postgresqlContainer = new PostgreSQLContainer<T>(dockerImageName)
                .withAccessToHost(true)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withMemory(512 * 1024 * 1024L))
                .withExposedPorts(5432)
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withNetworkMode("host");

        postgresqlContainer.setPortBindings(Lists.newArrayList("5432:5432"));
        postgresqlContainer.start();

        Testcontainers.exposeHostPorts(5432);

        return postgresqlContainer;
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
    protected TransactionsCsvGenerator transactionsCsvGenerator() {
        return new TransactionsCsvGenerator();
    }

}
