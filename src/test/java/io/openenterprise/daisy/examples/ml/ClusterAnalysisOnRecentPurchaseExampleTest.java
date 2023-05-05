package io.openenterprise.daisy.examples.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.trivago.triava.tcache.EvictionPolicy;
import com.trivago.triava.tcache.core.Builder;
import io.openenterprise.daisy.examples.AbstractTest;
import io.openenterprise.daisy.examples.Configuration;
import io.openenterprise.daisy.examples.data.Gender;
import io.openenterprise.daisy.examples.data.MemberTier;
import io.openenterprise.daisy.spark.ml.amazonaws.AmazonS3ModelStorage;
import io.openenterprise.daisy.springframework.spark.convert.JsonNodeToDatasetConverter;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.ml.Transformer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.annotation.PostConstruct;
import javax.cache.Cache;
import javax.cache.Caching;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@TestPropertySource(properties = {"spring.profiles.active=example,local-spark"})
public class ClusterAnalysisOnRecentPurchaseExampleTest extends AbstractTest {

    @Autowired
    protected AmazonS3ModelStorage amazonS3ModelStorage;

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

        var modelId = clusterAnalysisOnRecentPurchaseExample.buildModel(dataset, Map.of(), amazonS3ModelStorage);

        assertNotNull(modelId);
        assertTrue(amazonS3.listObjects(daisyS3Bucket).getObjectSummaries().stream().allMatch(
                s3ObjectSummary -> s3ObjectSummary.getKey().contains("ml/models/" + modelId)));

        var jsonString = "{\"memberId\": \"" + UUID.randomUUID() + "\", " +
                "\"age\":" + RandomUtils.nextInt(0, 99) + ",  " +
                "\"gender\": \"" + Gender.values()[RandomUtils.nextInt(0, Gender.values().length)] + "\", " +
                "\"tier\": \"" + MemberTier.values()[RandomUtils.nextInt(0, MemberTier.values().length)] + "\", " +
                "\"skuCategory\":" + RandomUtils.nextInt(0, 5) + "}";

        var transformedDataset = clusterAnalysisOnRecentPurchaseExample.predict(modelId, jsonString,
                amazonS3ModelStorage);

        assertNotNull(transformedDataset);
        assertNotNull(transformedDataset.col("prediction"));
        assertNotNull(transformedDataset.select("prediction"));
    }

    @TestConfiguration
    public static class Configuration  {

        @Autowired
        protected Environment environment;

        @Autowired
        protected MySQLContainer mySQLContainer;

        @Autowired
        protected PostgreSQLContainer postgreSQLContainer;

        @Bean
        protected ClusterAnalysisOnRecentPurchaseExample clusterAnalysisOnRecentPurchaseExample() {
            return new ClusterAnalysisOnRecentPurchaseExample();
        }

        @Bean
        protected AmazonS3ModelStorage amazonS3ModelStorage() {
            return new AmazonS3ModelStorage();
        }

        @Bean
        protected JsonNodeToDatasetConverter jsonNodeToDatasetConverter() {
            return new JsonNodeToDatasetConverter();
        }

        @Bean
        protected ObjectMapper objectMapper() {
            return new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        @Bean
        protected Cache<String, Transformer> sparkModelCache() {
            var cachingProvider = Caching.getCachingProvider();
            var cacheManager = cachingProvider.getCacheManager();
            var builder = new Builder<String, Transformer>()
                    .setEvictionPolicy(EvictionPolicy.LFU);

            return cacheManager.createCache("sparkModelCache", builder);
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