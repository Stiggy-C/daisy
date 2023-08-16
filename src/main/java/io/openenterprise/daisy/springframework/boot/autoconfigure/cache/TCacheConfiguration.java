package io.openenterprise.daisy.springframework.boot.autoconfigure.cache;

import com.trivago.triava.tcache.EvictionPolicy;
import com.trivago.triava.tcache.core.Builder;
import io.openenterprise.daisy.spark.MvelExpressionService;
import org.apache.spark.ml.Transformer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.cache.Cache;
import javax.cache.Caching;

@Configuration
@Profile("tCache")
public class TCacheConfiguration {

    @Bean
    protected Cache<String, MvelExpressionService.Session> expressionServiceSessionsCache() {
        var cachingProvider = Caching.getCachingProvider();
        var cacheManager = cachingProvider.getCacheManager();
        var builder = new Builder<String, MvelExpressionService.Session>()
                .setEvictionPolicy(EvictionPolicy.LFU);

        return cacheManager.createCache("expressionServiceSessionsCache", builder);
    }

    @Bean
    protected Cache<String, Transformer> sparkMlModelCache() {
        var cachingProvider = Caching.getCachingProvider();
        var cacheManager = cachingProvider.getCacheManager();
        var builder = new Builder<String, Transformer>()
                .setEvictionPolicy(EvictionPolicy.LFU);

        return cacheManager.createCache("sparkMlModelCache", builder);
    }

}
