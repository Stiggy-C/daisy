package io.openenterprise.daisy.example.spark.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.openenterprise.daisy.Invocation;
import io.openenterprise.daisy.InvocationContext;
import io.openenterprise.daisy.MvelDrivenOperation;
import io.openenterprise.daisy.MvelDrivenOperationImpl;
import io.openenterprise.daisy.example.AbstractTest;
import io.openenterprise.daisy.spark.ml.amazonaws.AmazonS3ModelStorage;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.spark.ApplicationConfiguration;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.ClassUtils;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import javax.cache.Cache;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static io.openenterprise.daisy.Parameter.SESSION_ID;
import static io.openenterprise.daisy.spark.sql.Parameter.DATASET_PATH;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@Import({TCacheConfiguration.class, ApplicationConfiguration.class, HongKongMark6ResultClusterAnalysisCompositeOperationTest.Configuration.class})
@TestPropertySource(properties = {"daisy.s3.bucket=daisy", "spring.profiles.active=local_spark,ml_example,spark,tCache"})
public class HongKongMark6ResultClusterAnalysisCompositeOperationTest extends AbstractTest {

    @Autowired
    protected AmazonS3ModelStorage amazonS3ModelStorage;

    @Autowired
    protected Cache<UUID, InvocationContext> invocationContextCache;

    @Autowired
    protected HongKongMark6ResultClusterAnalysisCompositeOperation hongKongMark6ResultClusterAnalysisCompositeOperation;

    @Value("${daisy.s3.bucket}")
    protected String daisyS3Bucket;

    @Test
    public void test() throws JsonProcessingException, AnalysisException {
        var sessionId = UUID.randomUUID();

        Map<String, Object> parameter = Map.of(
                DATASET_PATH.getKey(), "s3a://" + TEST_S3_BUCKET + "/csv_files/hk_mark_6_results.csv",
                SESSION_ID.getKey(), sessionId);

        assertDoesNotThrow(() -> hongKongMark6ResultClusterAnalysisCompositeOperation.invoke(parameter));
        assertTrue(invocationContextCache.containsKey(sessionId));

        var invocationContext = invocationContextCache.get(sessionId);

        assertEquals(5, invocationContext.getPastInvocations().size());

        var model = invocationContext.getPastInvocations().stream()
                .filter(invocation -> Objects.nonNull(invocation.getResult()))
                .filter(invocation -> ClassUtils.isAssignable(invocation.getResult().getClass(), KMeansModel.class))
                .map(Invocation::getResult)
                .map(KMeansModel.class::cast)
                .findAny()
                .orElse(null);

        assertNotNull(model);

        var cluster = model.summary().cluster();

        cluster.show((int) cluster.count());

        var finalResult = invocationContext.getCurrentInvocation().getResult();

        assertNotNull(finalResult);
        assertTrue(ClassUtils.isAssignable(finalResult.getClass(), Dataset.class));

        @SuppressWarnings("unchecked")
        var dataset = (Dataset<Row>) finalResult;

        assertNotNull(dataset.col("prediction"));

        dataset.orderBy(col("count").desc()).show((int) dataset.count());
    }

    @PostConstruct
    protected void postConstruct() throws IOException {
        super.postConstruct();

        var hkMark6ResultsCsvUri = "file://" + System.getProperty("user.dir") +
                "/example/hk_mark_6_results_20080103-.csv";
        var csvFile = ResourceUtils.getFile(hkMark6ResultsCsvUri);

        amazonS3.createBucket(TEST_S3_BUCKET);
        amazonS3.putObject(TEST_S3_BUCKET, "csv_files/hk_mark_6_results.csv", csvFile);
    }

    @TestConfiguration
    protected static class Configuration {

        @Bean
        protected HongKongMark6ResultClusterAnalysisCompositeOperation hongKongMark6ResultClusterAnalysisCompositeOperation() {
            return new HongKongMark6ResultClusterAnalysisCompositeOperation();
        }
    }
}

