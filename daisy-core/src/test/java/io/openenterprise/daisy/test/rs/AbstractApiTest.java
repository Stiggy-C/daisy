package io.openenterprise.daisy.test.rs;

import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.test.AbstractTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@Import({AbstractApiTest.Configuration.class, TCacheConfiguration.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractApiTest extends AbstractTest {

    protected static final String CONTEXT_PATH = "/services";

    @Value("${local.server.port}")
    protected int port;

    @Autowired
    protected TestRestTemplate testRestTemplate;

    @TestConfiguration
    protected static class Configuration {

        @Bean
        protected TestRestTemplate testRestTemplate() {
            return new TestRestTemplate();
        }
    }
}
