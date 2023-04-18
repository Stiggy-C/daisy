package io.openenterprise.daisy.examples.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.openenterprise.daisy.examples.AbstractTest;
import io.openenterprise.daisy.examples.BaseTestConfiguration;
import io.openenterprise.daisy.examples.data.Gender;
import io.openenterprise.daisy.examples.data.MemberTier;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {"spring.profiles.active=example,local-spark"})
class ClusterAnalysisOnRecentPurchaseExampleTest extends AbstractTest {

    @Autowired
    protected ClusterAnalysisOnRecentPurchaseExample clusterAnalysisOnRecentPurchaseExample;

    @Value("${daisy.s3.bucket}")
    protected String daisyS3Bucket;

    @Test
    public void test() throws Exception {
        var dataset = clusterAnalysisOnRecentPurchaseExample.buildDataset(Map.of("csvS3Uri",
                "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv"));

        assertNotNull(dataset);
        assertFalse(dataset.isEmpty());

        var modelId = clusterAnalysisOnRecentPurchaseExample.buildAndStoreModel(dataset, Map.of());

        assertNotNull(modelId);
        assertTrue(amazonS3.listObjects(daisyS3Bucket).getObjectSummaries().stream().allMatch(
                s3ObjectSummary -> s3ObjectSummary.getKey().contains("ml/models/" + modelId)));

        var jsonString = "{\"memberId\": \"" + UUID.randomUUID() + "\", " +
                "\"age\":" + RandomUtils.nextInt(0, 99) + ",  " +
                "\"gender\": \"" + Gender.values()[RandomUtils.nextInt(0, Gender.values().length)] + "\", " +
                "\"tier\": \"" + MemberTier.values()[RandomUtils.nextInt(0, MemberTier.values().length)] + "\", " +
                "\"skuCategory\":" + RandomUtils.nextInt(0, 5) + "}";

        var transformedDataset = clusterAnalysisOnRecentPurchaseExample.predict(modelId, jsonString);

        assertNotNull(transformedDataset);
        assertNotNull(transformedDataset.col("prediction"));
        assertNotNull(transformedDataset.select("prediction"));
    }


    @TestConfiguration
    protected static class Configuration extends BaseTestConfiguration {

        @Bean
        protected ClusterAnalysisOnRecentPurchaseExample clusterAnalysisOnRecentPurchaseExample() {
            return new ClusterAnalysisOnRecentPurchaseExample();
        }

        @Bean
        protected ObjectMapper objectMapper() {
            return new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        @PostConstruct
        protected void postConstruct() {
            var properties = new Properties();
            properties.setProperty("clusterAnalysisOnRecentPurchaseExample.mySqlJdbcPassword", mySQLContainer.getPassword());
            properties.setProperty("clusterAnalysisOnRecentPurchaseExample.mySqlJdbcUrl", mySQLContainer.getJdbcUrl());
            properties.setProperty("clusterAnalysisOnRecentPurchaseExample.mySqlJdbcUser", mySQLContainer.getUsername());
            properties.setProperty("daisy.s3.bucket", "daisy");

            ((ConfigurableEnvironment) environment).getPropertySources()
                    .addLast(new PropertiesPropertySource(ClusterAnalysisOnRecentPurchaseExample.class.getName(), properties));

        }
    }

}