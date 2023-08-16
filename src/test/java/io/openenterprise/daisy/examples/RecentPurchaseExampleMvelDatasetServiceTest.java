package io.openenterprise.daisy.examples;

import com.amazonaws.services.s3.AmazonS3;
import io.openenterprise.daisy.Parameters;
import io.openenterprise.daisy.spark.sql.MvelExpressionServiceTest;
import io.openenterprise.daisy.springframework.core.io.support.YamlPropertySourceFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@Import(RecentPurchaseExampleMvelDatasetServiceTest.Configuration.class)
@PropertySource(value = "classpath:recentPurchaseExampleMvelPipeline.yaml", factory = YamlPropertySourceFactory.class)
@TestPropertySource(properties = {"spring.profiles.active=local_spark,pipeline_example"})
class RecentPurchaseExampleMvelDatasetServiceTest extends AbstractTest {

    @Autowired
    protected AmazonS3 amazonS3;

    @Autowired
    @Qualifier("postgresDatasource")
    protected DataSource dataSource;

    @Autowired
    protected RecentPurchaseExampleMvelDatasetService recentPurchaseExampleMvelDatasetService;

    @Test
    void test() {
        var parameters = Map.of(
                "csvS3Uri", "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv",
                Parameters.MVEL_CLASS_IMPORTS.getName(), new String[]{},
                Parameters.MVEL_PACKAGE_IMPORTS.getName(), new String[]{"java.io", "java.net", "java.nio.file",
                        "java.util", "java.util.stream", "scala.collection", "com.amazonaws.services.s3",
                        "com.google.common.collect", "io.openenterprise.daisy", "org.apache.commons.collections4",
                        "org.apache.spark.sql", "plotly", "plotly.layout"},
                Parameters.SESSION_ID.getName(), UUID.randomUUID().toString(),
                "plotS3Uri", "s3://" + TEST_S3_BUCKET + "/plots/recentPurchaseExampleMvelDatasetService.html"
        );

        var dataset = recentPurchaseExampleMvelDatasetService.buildDataset(parameters);

        Assertions.assertNotNull(dataset);
        Assertions.assertFalse(dataset.isEmpty());

        recentPurchaseExampleMvelDatasetService.writeDataset(dataset, parameters);

        var jdbcTemplate = new JdbcTemplate(dataSource);
        var numberOfRecentPurchases = jdbcTemplate.queryForObject("select count(*) from recent_purchases",
                Long.class);

        assertNotNull(numberOfRecentPurchases);
        assertEquals(dataset.count(), numberOfRecentPurchases);

        recentPurchaseExampleMvelDatasetService.plot(dataset, parameters);

        assertTrue(amazonS3.doesObjectExist(TEST_S3_BUCKET, "plots/recentPurchaseExampleMvelDatasetService.html"));

        var plotSettings = recentPurchaseExampleMvelDatasetService.toPlotJson(dataset, parameters);

        assertNotNull(plotSettings);
    }

    @PostConstruct
    protected void postConstruct() throws IOException {
        generateExampleTransactions();
    }

    @TestConfiguration
    protected static class Configuration extends MvelExpressionServiceTest.Configuration {

        @Autowired
        protected Environment environment;

        @Autowired
        protected MySQLContainer mySQLContainer;

        @Autowired
        protected PostgreSQLContainer postgreSQLContainer;

        @Bean
        protected RecentPurchaseExampleMvelDatasetService recentPurchaseExampleMvelDatasetService() {
            return new RecentPurchaseExampleMvelDatasetService();
        }

        @PostConstruct
        private void postConstruct() {
            var properties = new Properties();
            properties.setProperty("recentPurchaseExampleMvelPipeline.my-sql-jdbc-password", mySQLContainer.getPassword());
            properties.setProperty("recentPurchaseExampleMvelPipeline.my-sql-jdbc-url", mySQLContainer.getJdbcUrl());
            properties.setProperty("recentPurchaseExampleMvelPipeline.my-sql-jdbc-user", mySQLContainer.getUsername());
            properties.setProperty("recentPurchaseExampleMvelPipeline.postgres-jdbc-password", postgreSQLContainer.getPassword());
            properties.setProperty("recentPurchaseExampleMvelPipeline.postgres-jdbc-url", postgreSQLContainer.getJdbcUrl());
            properties.setProperty("recentPurchaseExampleMvelPipeline.postgres-jdbc-user", postgreSQLContainer.getUsername());

            ((ConfigurableEnvironment) environment).getPropertySources()
                    .addLast(new PropertiesPropertySource(RecentPurchaseExampleMvelDatasetService.class.getName(), properties));
        }
    }

}