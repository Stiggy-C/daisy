package io.openenterprise.daisy.spark.sql.rs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import io.openenterprise.daisy.examples.ml.RecentPurchaseExampleClusterAnalysisTest;
import io.openenterprise.daisy.examples.ml.PmmlBasedMachineLearningExampleTest;
import io.openenterprise.daisy.rs.model.TrainingResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.*;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@EnableAutoConfiguration
@Import({RecentPurchaseExampleClusterAnalysisTest.Configuration.class,
        PmmlBasedMachineLearningExampleTest.Configuration.class, AbstractApiTest.Configuration.class,
        MlApiImplTest.Configuration.class})
@TestPropertySource(properties = {"spring.main.allow-bean-definition-overriding=true", "spring.profiles.active=local_spark,ml_example"})
class MlApiImplTest extends AbstractApiTest {

    @Test
    void test() throws Exception {
        var header = new HttpHeaders();
        header.setContentType(MediaType.APPLICATION_JSON);

        var requestBody0 = Map.of("csvS3Uri",
                "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv");
        var urlString = "http://localhost:" + port + CONTEXT_PATH + "/ml/recentPurchaseExampleClusterAnalysis/train";

        var responseEntity = testRestTemplate.exchange(urlString, HttpMethod.POST,
                new HttpEntity<>(requestBody0, header), TrainingResponse.class);

        assertNotNull(responseEntity);
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
        assertTrue(responseEntity.hasBody());
        assertNotNull(responseEntity.getBody());
        assertNotNull(responseEntity.getBody().getModelId());
        assertNotNull(responseEntity.getBody().getModelUri());

        var modelId = responseEntity.getBody().getModelId();

        var requestBody1 = RecentPurchaseExampleClusterAnalysisTest.JSON_STRING;
        urlString = "http://localhost:" + port + CONTEXT_PATH +
                "/ml/recentPurchaseExampleClusterAnalysis/predict?modelId=" + modelId;

        var responseEntity1 = testRestTemplate.exchange(urlString, HttpMethod.POST,
                new HttpEntity<>(requestBody1, header), String.class);

        assertNotNull(responseEntity1);
        assertEquals(HttpStatus.OK, responseEntity1.getStatusCode());
        assertTrue(responseEntity1.hasBody());
        assertNotNull(responseEntity1.getBody());
    }

    @TestConfiguration
    protected static class Configuration {

        @Autowired
        protected ObjectMapper objectMapper;

        @Bean
        protected JacksonJsonProvider jacksonJsonProvider() {
            return new JacksonJsonProvider(objectMapper);
        }
    }
}
