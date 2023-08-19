package io.openenterprise.daisy.examples;

import io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
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
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {"spring.profiles.active=local_spark,pipeline_example"})
class RecentPurchaseExampleStreamingDatasetComponentTest extends AbstractTest {

    @Autowired
    @Qualifier("postgresDatasource")
    protected DataSource dataSource;

    @Autowired
    protected RecentPurchaseExampleStreamingDatasetComponent recentPurchaseExampleStreamingPipeline;

    @Test
    public void test() throws TimeoutException, AnalysisException, StreamingQueryException {
        var dataset = recentPurchaseExampleStreamingPipeline.buildDataset(Map.of(),
                CreateTableOrViewPreference.CREATE_OR_REPLACE_GLOBAL_VIEW);

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

        assertNotNull(sparkSession.table(recentPurchaseExampleStreamingPipeline.getClass().getSimpleName()));

    }

    @PostConstruct
    protected void postConstruct() throws IOException {
        generateExampleTransactions();
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
        protected RecentPurchaseExampleStreamingDatasetComponent recentPurchaseExampleStreamingPipeline() {
            return new RecentPurchaseExampleStreamingDatasetComponent();
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
                    .addLast(new PropertiesPropertySource(RecentPurchaseExampleDatasetComponent.class.getName(), properties));
        }
    }
}