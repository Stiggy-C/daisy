package io.openenterprise.daisy.springframework.boot.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.Map;

@Configuration
public class ApplicationConfiguration {

    @Autowired
    protected ApplicationContext applicationContext;

    @Autowired
    protected ObjectMapper objectMapper;

    @Bean
    @Qualifier("builtInMvelVariables")
    protected Map<String, Object> builtInMvelVariables() {
        return Maps.newHashMap(Map.of("applicationContext", applicationContext, "objectMapper", objectMapper));
    }

    @Bean
    protected HttpClient httpClient() {
        return HttpClients.createDefault();
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
