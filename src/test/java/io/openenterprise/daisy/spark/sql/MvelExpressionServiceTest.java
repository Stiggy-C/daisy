package io.openenterprise.daisy.spark.sql;

import com.trivago.triava.tcache.EvictionPolicy;
import com.trivago.triava.tcache.core.Builder;
import io.openenterprise.daisy.Parameters;
import io.openenterprise.daisy.examples.AbstractTest;
import io.openenterprise.daisy.spark.MvelExpressionService;
import org.apache.ivy.util.StringUtils;
import org.apache.spark.sql.Dataset;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mvel2.ParserConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.Order;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.cache.Cache;
import javax.cache.Caching;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@Import(MvelExpressionServiceTest.Configuration.class)
@TestPropertySource(properties = {"spring.profiles.active=local_spark"})
public class MvelExpressionServiceTest extends AbstractTest {

    @Inject
    protected Cache<String, MvelExpressionService.Session> sessionsCache;

    @Inject
    protected MvelExpressionService mvelExpressionService;

    @Inject
    protected ParserConfiguration parserConfiguration;

    @Test
    void testEvaluateWithException() {
        var sessionId = UUID.randomUUID().toString();

        try {
            mvelExpressionService.evaluate("$notACorrentMvELExpression", getParameters(sessionId));
        } catch (Exception e) {
            // Do nothing.
        }

        assertTrue(sessionsCache.containsKey(sessionId));
        assertFalse(sessionsCache.get(sessionId).getEvaluationHistory().isEmpty());
        Assertions.assertNotNull(sessionsCache.get(sessionId).getEvaluationHistory().get(0).getException());
    }

    @Test
    void testEvaluateWithOutException() {
        var sessionId = UUID.randomUUID().toString();
        var dataset = mvelExpressionService.evaluate(getExpression0(), getParameters(sessionId));

        assertNotNull(dataset);

        assertTrue(sessionsCache.containsKey(sessionId));
        assertFalse(sessionsCache.get(sessionId).getEvaluationHistory().isEmpty());

        var count = mvelExpressionService.evaluate(getExpression1(), getParameters(sessionId));

        assertNotNull(count);
        assertTrue(count instanceof Long);
    }

    @NotNull
    protected String getExpression0() {
        return "Dataset dataset = spark.read().format(\"csv\")" +
                ".option(\"header\", \"true\")" +
                ".option(\"inferSchema\", \"true\")" +
                ".load(parameters[\"csvS3Uri\"])" +
                ".select(\"memberId\", \"skuId\", \"skuCategory\", \"skuPrice\", \"createdDateTime\")" +
                ".as(\"csv\");";
    }

    protected String getExpression1() {
        return "dataset.count()";
    }

    @NotNull
    private Map<String, ?> getParameters(@Nonnull String sessionId) {
        return Map.of("csvS3Uri", "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv",
                Parameters.SESSION_ID.getName(), sessionId,
                Parameters.DATASET_VIEW.getName(), StringUtils.uncapitalize(getClass().getSimpleName()));
    }

    @PostConstruct
    protected void postConstruct() throws IOException {
        generateExampleTransactions();
    }


    @TestConfiguration
    public static class Configuration {

        @Bean("datasetServicesMap")
        @Order
        protected Map<String, BaseDatasetService> datasetServiceMap(@Nonnull ApplicationContext applicationContext) {
            return applicationContext.getBeansOfType(BaseDatasetService.class);
        }

        @Bean
        protected Cache<String, MvelExpressionService.Session> expressionServiceSessionsCache() {
            var cachingProvider = Caching.getCachingProvider();
            var cacheManager = cachingProvider.getCacheManager();
            var builder = new Builder<String, MvelExpressionService.Session>()
                    .setEvictionPolicy(EvictionPolicy.LFU);

            return cacheManager.createCache("expressionServiceSessionsCache", builder);
        }

        @Bean
        protected MvelExpressionService expressionService() {
            return Mockito.spy(new MvelExpressionService());
        }

        @Bean
        protected ParserConfiguration parserConfiguration() {
            var parserConfiguration = new ParserConfiguration();
            parserConfiguration.addImport("DaisyConstants", Parameters.class);
            parserConfiguration.addImport("Dataset", Dataset.class);

            return parserConfiguration;
        }
    }
}