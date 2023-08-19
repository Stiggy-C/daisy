package io.openenterprise.daisy.springframework.boot.autoconfigure;

import io.openenterprise.daisy.spark.sql.BaseDatasetComponent;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import javax.annotation.Nonnull;
import java.util.Map;

@Configuration
public class ApplicationConfiguration {

    @Bean
    protected ExpressionParser expressionParser() {
        return new SpelExpressionParser();
    }

    @Bean
    protected HttpClient httpClient() {
        return HttpClients.createDefault();
    }

    @Bean("datasetServicesMap")
    @Order
    protected Map<String, BaseDatasetComponent> datasetServiceMap(@Nonnull ApplicationContext applicationContext) {
        return applicationContext.getBeansOfType(BaseDatasetComponent.class);
    }
}
