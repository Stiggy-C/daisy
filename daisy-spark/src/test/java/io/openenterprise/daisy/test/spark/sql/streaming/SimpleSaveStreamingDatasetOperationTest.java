package io.openenterprise.daisy.test.spark.sql.streaming;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Maps;
import io.openenterprise.daisy.spark.sql.streaming.SimpleLoadStreamingDatasetOperation;
import io.openenterprise.daisy.spark.sql.streaming.SimpleSaveStreamingDatasetOperation;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.spark.ApplicationConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.streaming.StreamingQueryException;
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
class SimpleSaveStreamingDatasetOperationTest extends AbstractTest {

    @Inject
    protected AmazonS3 amazonS3;

    @Inject
    protected SimpleLoadStreamingDatasetOperation simpleLoadStreamingDatasetOperation;

    @Inject
    protected SimpleSaveStreamingDatasetOperation simpleSaveStreamingDatasetOperation;

    @Test
    void testInvokeAwsS3CsvOk() throws StreamingQueryException {
        var sessionId = UUID.randomUUID();

        var parameters0 = Maps.newHashMap(Map.<String, Object>of(
                CSV_HEADER.getKey(), true,
                CSV_INFER_SCHEMA.getKey(), true,
                DATASET_FORMAT.getKey(), "csv",
                DATASET_PATH.getKey(), "s3a://" + TEST_S3_BUCKET + "/csv_files",
                SESSION_ID.getKey(), sessionId));

        var dataset = simpleLoadStreamingDatasetOperation.invoke(parameters0);

        assertNotNull(dataset);
        assertTrue(dataset.isStreaming());

        var parameters1 = Maps.newHashMap(Map.<String, Object>of(
                CSV_HEADER.getKey(), true,
                CSV_INFER_SCHEMA.getKey(), true,
                DATASET_FORMAT.getKey(), "csv",
                DATASET_PATH.getKey(), "s3a://" + TEST_S3_BUCKET + "/csv_files_cloned",
                DATASET_VARIABLE.getKey(), parameters0.get(DATASET_VARIABLE.getKey()),
                SESSION_ID.getKey(), sessionId));

        var streamingQuery = simpleSaveStreamingDatasetOperation.invoke(parameters1);

        assertNotNull(streamingQuery);

        streamingQuery.awaitTermination(10000);

        assertTrue(CollectionUtils.isNotEmpty(
                amazonS3.listObjects(TEST_S3_BUCKET, "csv_files_cloned").getObjectSummaries()));
    }
}