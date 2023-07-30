package io.openenterprise.daisy.spark.sql;

import com.trivago.triava.tcache.EvictionPolicy;
import com.trivago.triava.tcache.core.Builder;
import io.openenterprise.daisy.Constants;
import io.openenterprise.daisy.examples.AbstractTest;
import io.openenterprise.daisy.spark.ExpressionService;
import org.apache.ivy.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.Order;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.cache.Cache;
import javax.cache.Caching;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@Import(ExpressionServiceTest.Configuration.class)
@TestPropertySource(properties = {"spring.profiles.active=local_spark"})
class ExpressionServiceTest extends AbstractTest {

    @Inject
    protected Cache<String, ExpressionService.Session> sessionsCache;

    @Inject
    protected ExpressionService expressionService;

    @Test
    void testEvaluateWithException() {
        Expression expression = expressionService.parseExpression("$notASpelExpression");

        var sessionId = UUID.randomUUID().toString();
        EvaluationContext evaluationContext = getEvaluationContext(sessionId);

        try {
            expressionService.evaluate(expression, evaluationContext);
        } catch (Exception e) {
            // Do nothing.
        }

        Assertions.assertTrue(sessionsCache.containsKey(sessionId));
        Assertions.assertFalse(sessionsCache.get(sessionId).getEvaluationHistory().isEmpty());
        Assertions.assertNotNull(sessionsCache.get(sessionId).getEvaluationHistory().get(0).getException());
    }

    @Test
    void testEvaluateWithOutException() {
        Expression expression = getExpression();

        var sessionId = UUID.randomUUID().toString();
        EvaluationContext evaluationContext = getEvaluationContext(sessionId);

        var dataset = expressionService.evaluate(expression, evaluationContext);

        assertNotNull(dataset);

        Assertions.assertTrue(sessionsCache.containsKey(sessionId));
        Assertions.assertFalse(sessionsCache.get(sessionId).getEvaluationHistory().isEmpty());
    }

    @NotNull
    private EvaluationContext getEvaluationContext(@Nonnull String sessionId) {
        return expressionService.buildEvaluationContext(ImmutableMap.of(
                "csvS3Uri", "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv",
                Constants.SESSION_ID_PARAMETER_NAME.getValue(), sessionId,
                Constants.VIEW_NAME_PARAMETER_NAME.getValue(), StringUtils.uncapitalize(getClass().getSimpleName())));
    }

    @NotNull
    protected Expression getExpression() {
        var expressionString = "#spark.read().format(\"csv\")" +
                ".option(\"header\", \"true\")\n" +
                ".option(\"inferSchema\", \"true\")" +
                ".load(#parameters['csvS3Uri'])\n" +
                ".select(\"memberId\", \"skuId\", \"skuCategory\", \"skuPrice\", \"createdDateTime\")\n" +
                ".as(\"csv\")";

        return expressionService.parseExpression(expressionString);
    }

    @PostConstruct
    protected void postConstruct() throws IOException {
        generateExampleTransactions();
    }


    @TestConfiguration
    protected static class Configuration {

        @Bean("datasetServicesMap")
        @Order
        protected Map<String, BaseDatasetService> datasetServiceMap(@Nonnull ApplicationContext applicationContext) {
            return applicationContext.getBeansOfType(BaseDatasetService.class);
        }

        @Bean
        protected Cache<String, ExpressionService.Session> expressionServiceSessionsCache() {
            var cachingProvider = Caching.getCachingProvider();
            var cacheManager = cachingProvider.getCacheManager();
            var builder = new Builder<String, ExpressionService.Session>()
                    .setEvictionPolicy(EvictionPolicy.LFU);

            return cacheManager.createCache("expressionServiceSessionsCache", builder);
        }

        @Bean
        protected ExpressionParser expressionParser() {
            return new SpelExpressionParser();
        }

        @Bean
        protected ExpressionService expressionService() {
            return Mockito.spy(new ExpressionService());
        }
    }
}