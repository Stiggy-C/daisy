package io.openenterprise.daisy.springframework.boot.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Maps;
import io.openenterprise.daisy.InvocationContextUtils;
import io.openenterprise.daisy.springframework.context.support.ApplicationContextUtils;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.mvel2.ParserConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nonnull;
import java.util.Map;

@Configuration
public class ApplicationConfiguration {

    @Bean
    @Qualifier("builtInMvelVariables")
    protected Map<String, Object> builtInMvelVariables(@Nonnull ApplicationContext applicationContext,
                                                       @Nonnull ObjectMapper objectMapper) {
        return Maps.newHashMap(Map.of("applicationContext", applicationContext, "objectMapper", objectMapper));
    }

    @Bean
    protected HttpClient httpClient() {
        return HttpClients.createDefault();
    }

    @Bean
    protected ObjectMapper objectMapper() {
        return new ObjectMapper().findAndRegisterModules().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    protected ParserConfiguration parserConfiguration() {
        var parserConfiguration = new ParserConfiguration();

        // Add common utils:
        parserConfiguration.addPackageImport("com.google.common.collect");
        parserConfiguration.addPackageImport("org.apache.commons.collections4");
        parserConfiguration.addPackageImport("org.apache.commons.lang3");

        // Add Daisy utils:
        parserConfiguration.addImport(InvocationContextUtils.class);
        parserConfiguration.addImport(ApplicationContextUtils.class);

        return parserConfiguration;
    }

}
