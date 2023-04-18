package io.openenterprise.daisy.examples;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {"spring.profiles.active=example,local-spark"})
class RecentPurchaseExamplePipelineTest extends AbstractTest {

    @Autowired
    @Qualifier("postgresDatasource")
    protected DataSource dataSource;

    @Autowired
    protected RecentPurchaseExamplePipeline recentPurchaseExamplePipeline;

    @Test
    public void test() {
        var dataset = recentPurchaseExamplePipeline.buildDataset(Map.of("csvS3Uri",
                "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv"));

        assertNotNull(dataset);
        assertFalse(dataset.isEmpty());

        assertDoesNotThrow(() -> recentPurchaseExamplePipeline.writeDataset(dataset, Map.of()));

        var jdbcTemplate = new JdbcTemplate(dataSource);
        var numberOfRecentPurchases = jdbcTemplate.queryForObject("select count(*) from recent_purchases",
                Long.class);

        assertNotNull(numberOfRecentPurchases);
        assertTrue(numberOfRecentPurchases > 0);
        assertEquals(dataset.count(), numberOfRecentPurchases);
    }

    @org.springframework.boot.test.context.TestConfiguration
    protected static class ExampleTestConfiguration extends BaseTestConfiguration {

        @Bean
        protected RecentPurchaseExamplePipeline recentPurchaseExamplePipeline() {
            return new RecentPurchaseExamplePipeline();
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
                    .addLast(new PropertiesPropertySource(RecentPurchaseExamplePipeline.class.getName(), properties));
        }
    }
}