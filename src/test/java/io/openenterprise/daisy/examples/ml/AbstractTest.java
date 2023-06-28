package io.openenterprise.daisy.examples.ml;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.trivago.triava.tcache.EvictionPolicy;
import com.trivago.triava.tcache.core.Builder;
import io.openenterprise.daisy.spark.ml.amazonaws.AmazonS3ModelStorage;
import io.openenterprise.daisy.springframework.spark.convert.JsonNodeToDatasetConverter;
import org.apache.spark.ml.Transformer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import javax.cache.Cache;
import javax.cache.Caching;

public class AbstractTest extends io.openenterprise.daisy.examples.AbstractTest {

    @TestConfiguration
    protected static class Configuration {

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
    }
}
