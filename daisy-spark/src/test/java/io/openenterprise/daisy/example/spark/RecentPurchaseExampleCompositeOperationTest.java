package io.openenterprise.daisy.example.spark;

import io.openenterprise.daisy.example.AbstractTest;
import io.openenterprise.daisy.spark.sql.Parameter;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.spark.ApplicationConfiguration;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
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

@ExtendWith(SpringExtension.class)
@Import({TCacheConfiguration.class, ApplicationConfiguration.class, RecentPurchaseExampleCompositeOperationTest.Configuration.class})
@TestPropertySource(properties = {"daisy.aws.s3.bucket=daisy", "spring.profiles.active=local_spark,ml_example,spark,tCache"})
class RecentPurchaseExampleCompositeOperationTest extends AbstractTest {

    @Autowired
    @Qualifier("postgresDatasource")
    protected DataSource dataSource;

    @Autowired
    protected RecentPurchaseExampleCompositeOperation recentPurchaseExampleCompositeOperation;

    @Test
    public void test() throws AnalysisException, IOException {
        Assertions.assertDoesNotThrow(() -> recentPurchaseExampleCompositeOperation.invoke(Map.of(
                Parameter.DATASET_PATH.getKey(), "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv")));

        var jdbcTemplate = new JdbcTemplate(dataSource);
        var count = jdbcTemplate.queryForObject("select count(*) from members_purchases", Long.class);

        Assertions.assertNotNull(count);
        Assertions.assertTrue(count > 0);
    }

    @TestConfiguration
    protected static class Configuration {

        @Autowired
        protected Environment environment;

        @Autowired
        protected MySQLContainer mySQLContainer;

        @Autowired
        protected PostgreSQLContainer postgreSQLContainer;

        @Bean
        protected RecentPurchaseExampleCompositeOperation recentPurchaseExampleCompositeOperation() {
            return new RecentPurchaseExampleCompositeOperation();
        }

        @PostConstruct
        private void postConstruct() {
            var properties = new Properties();
            properties.setProperty("recentPurchaseExamplePipeline.mySqlJdbcPassword", mySQLContainer.getPassword());
            properties.setProperty("recentPurchaseExamplePipeline.mySqlJdbcUrl", mySQLContainer.getJdbcUrl());
            properties.setProperty("recentPurchaseExamplePipeline.mySqlJdbcUser", mySQLContainer.getUsername());
            properties.setProperty("recentPurchaseExamplePipeline.postgresJdbcPassword", postgreSQLContainer.getPassword());
            properties.setProperty("recentPurchaseExamplePipeline.postgresJdbcUrl", postgreSQLContainer.getJdbcUrl());
            properties.setProperty("recentPurchaseExamplePipeline.postgresJdbcUser", postgreSQLContainer.getUsername());

            ((ConfigurableEnvironment) environment).getPropertySources()
                    .addLast(new PropertiesPropertySource(RecentPurchaseExampleCompositeOperation.class.getName(), properties));
        }
    }
}