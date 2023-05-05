package io.openenterprise.daisy.examples.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.trivago.triava.tcache.EvictionPolicy;
import com.trivago.triava.tcache.core.Builder;
import io.openenterprise.daisy.ApplicationConfiguration;
import io.openenterprise.daisy.Configuration;
import io.openenterprise.daisy.springframework.spark.convert.JsonNodeToDatasetConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pmml4s.spark.ScoreModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.cache.Cache;
import javax.cache.Caching;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@Import({ApplicationConfiguration.class, Configuration.class, PmmlBasedMachineLearningExampleTest.Configuration.class})
@TestPropertySource(properties = {"spring.profiles.active=example,local-spark"})
class PmmlBasedMachineLearningExampleTest {

    @Autowired
    protected PmmlBasedMachineLearningExample pmmlBasedMachineLearningExample;

    @Test
    public void test() throws IOException {
        var modelId = pmmlBasedMachineLearningExample
                .importModel("http://dmg.org/pmml/pmml_examples/KNIME_PMML_4.1_Examples/single_iris_dectree.xml");

        assertNotNull(modelId);

        var dataset = pmmlBasedMachineLearningExample.predict(modelId,
                "{\"sepal_length\": 5.1, \"sepal_width\": 3.5, \"petal_length\": 1.4, \"petal_width\": 0.2, \"class\": \"Iris-setosa\"}");

        assertNotNull(dataset);
        assertFalse(dataset.isEmpty());
    }

    @TestConfiguration
    protected static class Configuration {

        @Bean
        protected JsonNodeToDatasetConverter jsonNodeToDatasetConverter() {
            return new JsonNodeToDatasetConverter();
        }

        @Bean
        protected ObjectMapper objectMapper() {
            return new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        @Bean
        protected PmmlBasedMachineLearningExample pmmlBasedMachineLearningExample() {
            return new PmmlBasedMachineLearningExample();
        }

        @Bean
        protected Cache<String, ScoreModel> pmmlModelCache() {
            var cachingProvider = Caching.getCachingProvider();
            var cacheManager = cachingProvider.getCacheManager();
            var builder = new Builder<String, ScoreModel>()
                    .setEvictionPolicy(EvictionPolicy.LFU);

            return cacheManager.createCache("pmmlModelCache", builder);
        }
    }
}