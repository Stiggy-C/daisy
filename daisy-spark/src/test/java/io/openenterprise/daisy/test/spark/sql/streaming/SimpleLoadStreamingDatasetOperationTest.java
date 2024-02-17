package io.openenterprise.daisy.test.spark.sql.streaming;

import com.google.common.collect.Maps;
import io.openenterprise.daisy.spark.sql.streaming.SimpleLoadStreamingDatasetOperation;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.spark.ApplicationConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.inject.Inject;
import java.util.Map;
import java.util.UUID;

import static io.openenterprise.daisy.Parameter.SESSION_ID;
import static io.openenterprise.daisy.spark.sql.Parameter.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
@ExtendWith(SpringExtension.class)
@Import({TCacheConfiguration.class, ApplicationConfiguration.class})
@TestPropertySource(properties = {"spring.profiles.active=local_spark,pipeline_example,spark,tCache"})
class SimpleLoadStreamingDatasetOperationTest extends AbstractTest {

    @Inject
    protected SimpleLoadStreamingDatasetOperation simpleLoadStreamingDatasetOperation;

    @Test
    protected void testInvokeAwsS3CsvOk() {
        var parameters = Maps.newHashMap(Map.<String, Object>of(
                CSV_HEADER.getKey(), true,
                CSV_INFER_SCHEMA.getKey(), true,
                DATASET_FORMAT.getKey(), "csv",
                DATASET_PATH.getKey(), "s3a://" + TEST_S3_BUCKET + "/csv_files",
                SESSION_ID.getKey(), UUID.randomUUID()));

        var dataset = simpleLoadStreamingDatasetOperation.invoke(parameters);

        assertNotNull(dataset);
        assertTrue(dataset.isStreaming());
    }

}