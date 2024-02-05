package io.openenterprise.daisy.springframework.boot.autoconfigure.cache;

import com.trivago.triava.tcache.EvictionPolicy;
import com.trivago.triava.tcache.core.Builder;
import io.openenterprise.daisy.InvocationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.cache.Cache;
import javax.cache.Caching;
import java.util.UUID;

@Configuration
@Profile("tCache")
public class TCacheConfiguration {

    @Bean
    protected Cache<UUID, InvocationContext> invocationContextCache() {
        var cachingProvider = Caching.getCachingProvider();
        var cacheManager = cachingProvider.getCacheManager();
        var builder = new Builder<UUID, InvocationContext>()
                .setEvictionPolicy(EvictionPolicy.LFU);

        return cacheManager.createCache("invocationContextCache", builder);
    }

}
