package io.openenterprise.daisy.examples;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
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
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {"spring.profiles.active=example,local-spark"})
class RecentPurchaseExampleStreamingPipelineTest extends AbstractTest {

    @Autowired
    @Qualifier("postgresDatasource")
    protected DataSource dataSource;

    @Autowired
    protected RecentPurchaseExampleStreamingPipeline recentPurchaseExampleStreamingPipeline;

    @Test
    public void test() throws TimeoutException, StreamingQueryException {
        var dataset = recentPurchaseExampleStreamingPipeline.buildDataset(Map.of());

        assertNotNull(dataset);
        assertTrue(dataset.isStreaming());

        StreamingQuery streamingQuery = recentPurchaseExampleStreamingPipeline.writeDataset(dataset, Map.of());
        streamingQuery.awaitTermination(60000);

        assertNotNull(streamingQuery);

        var jdbcTemplate = new JdbcTemplate(dataSource);
        var numberOfRecentPurchases = jdbcTemplate.queryForObject("select count(*) from recent_purchases",
                Long.class);

        assertNotNull(numberOfRecentPurchases);
        assertTrue(numberOfRecentPurchases > 0);
    }

    @org.springframework.boot.test.context.TestConfiguration
    protected static class ExampleTestConfiguration extends BaseTestConfiguration {

        @Bean
        protected RecentPurchaseExampleStreamingPipeline recentPurchaseExampleStreamingPipeline() {
            return new RecentPurchaseExampleStreamingPipeline();
        }

        @PostConstruct
        private void postConstruct() {
            var properties = new Properties();
            properties.setProperty("recentPurchaseExampleStreamingPipeline.csvArchiveFolderS3Uri",
                    "s3a://" + TEST_S3_BUCKET + "/archives/recentPurchaseExampleStreamingPipeline");
            properties.setProperty("recentPurchaseExampleStreamingPipeline.csvFolderS3Uri",
                    "s3a://" + TEST_S3_BUCKET + "/csv_files");
            properties.setProperty("recentPurchaseExampleStreamingPipeline.mySqlJdbcPassword", mySQLContainer.getPassword());
            properties.setProperty("recentPurchaseExampleStreamingPipeline.mySqlJdbcUrl", mySQLContainer.getJdbcUrl());
            properties.setProperty("recentPurchaseExampleStreamingPipeline.mySqlJdbcUser", mySQLContainer.getUsername());
            properties.setProperty("recentPurchaseExampleStreamingPipeline.postgresJdbcPassword", postgreSQLContainer.getPassword());
            properties.setProperty("recentPurchaseExampleStreamingPipeline.postgresJdbcUrl", postgreSQLContainer.getJdbcUrl());
            properties.setProperty("recentPurchaseExampleStreamingPipeline.postgresJdbcUser", postgreSQLContainer.getUsername());
            properties.setProperty("recentPurchaseExampleStreamingPipeline.sparkCheckpointLocation",
                    "s3a://" + TEST_S3_BUCKET + "/checkpoints/recentPurchaseExampleStreamingPipeline");

            ((ConfigurableEnvironment) environment).getPropertySources()
                    .addLast(new PropertiesPropertySource(RecentPurchaseExamplePipeline.class.getName(), properties));
        }
    }
}