package io.openenterprise.daisy.examples;

import io.openenterprise.daisy.Parameters;
import io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.AnalysisException;
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
import java.nio.file.Files;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {"spring.profiles.active=local_spark,pipeline_example"})
class RecentPurchaseExampleDatasetServiceTest extends AbstractTest {

    @Autowired
    @Qualifier("postgresDatasource")
    protected DataSource dataSource;

    @Autowired
    protected RecentPurchaseExampleDatasetService recentPurchaseExamplePipeline;

    @Test
    public void test() throws AnalysisException, IOException {
        var dataset = recentPurchaseExamplePipeline.buildDataset(Map.of("csvS3Uri",
                "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv"), CreateTableOrViewPreference.CREATE_GLOBAL_VIEW);

        assertNotNull(dataset);
        assertFalse(dataset.isEmpty());

        assertDoesNotThrow(() -> recentPurchaseExamplePipeline.writeDataset(dataset, Map.of()));

        var jdbcTemplate = new JdbcTemplate(dataSource);
        var numberOfRecentPurchases = jdbcTemplate.queryForObject("select count(*) from recent_purchases",
                Long.class);

        assertNotNull(numberOfRecentPurchases);
        assertEquals(dataset.count(), numberOfRecentPurchases);

        assertNotNull(sparkSession.table("global_temp." + recentPurchaseExamplePipeline.getClass()
                .getSimpleName()));

        var plotData = recentPurchaseExamplePipeline.getPlotData(dataset, Map.of());

        assertNotNull(plotData);
        assertEquals(2, plotData.size());

        var plotSetting = recentPurchaseExamplePipeline.getPlotSetting(Map.of());

        assertNotNull(plotSetting);
        assertEquals("recentPurchaseExamplePipeline", plotSetting.getLayout().title().get());

        var s3ObjectName = "plots/recentPurchaseExamplePipeline_" + Instant.now().toEpochMilli() + ".html";

        recentPurchaseExamplePipeline.plot(dataset, Map.of(Parameters.PLOT_PATH.getName(), "s3://"
                + TEST_S3_BUCKET + "/" + s3ObjectName));

        assertTrue(amazonS3.doesObjectExist(TEST_S3_BUCKET, s3ObjectName));
        var path = Files.createTempFile("recentPurchaseExamplePipeline_" + Instant.now().toEpochMilli(),
                "html");
        var file = path.toFile();
        FileUtils.copyInputStreamToFile(amazonS3.getObject(TEST_S3_BUCKET, s3ObjectName).getObjectContent(), file);

        assertTrue(Files.size(path) > 0);
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
        protected RecentPurchaseExampleDatasetService recentPurchaseExamplePipeline() {
            return new RecentPurchaseExampleDatasetService();
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
                    .addLast(new PropertiesPropertySource(RecentPurchaseExampleDatasetService.class.getName(), properties));
        }
    }
}