package io.openenterprise.daisy.examples.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.openenterprise.daisy.Constants;
import io.openenterprise.daisy.spark.ml.amazonaws.AmazonS3ModelStorage;
import io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@Import(Configuration.class)
@TestPropertySource(properties = {"daisy.s3.bucket=daisy", "spring.profiles.active=local_spark,ml_example"})
public class HongKongMark6LotteryResultClusterAnalysisTest extends AbstractTest {

    private static final String JSON_STRING;

    static {
        JSON_STRING = "[" + IntStream.range(1, 50).mapToObj(i -> "{\"winning_number\": " + i + "}")
                .collect(Collectors.joining(",")) + "]";
    }

    @Autowired
    protected AmazonS3ModelStorage amazonS3ModelStorage;

    @Autowired
    protected HongKongMark6LotteryResultClusterAnalysis hongKongMark6LotteryResultClusterAnalysis;

    @Value("${daisy.s3.bucket}")
    protected String daisyS3Bucket;

    @Test
    public void test() throws JsonProcessingException, AnalysisException {
        var parameters = ImmutableMap.of(
                "csvS3Uri", "s3a://" + TEST_S3_BUCKET + "/csv_files/hk_mark_6_results.csv",
        Constants.FORMAT_PARAMETER_NAME.getValue(), "delta",
        Constants.PATH_PARAMETER_NAME.getValue(), "s3a://" + TEST_S3_BUCKET + "/delta_lake/hongKongMark6LotteryResultClusterAnalysis");

        var dataset = hongKongMark6LotteryResultClusterAnalysis.buildDataset(parameters,
                CreateTableOrViewPreference.CREATE_TABLE_OVERWRITE);

        assertNotNull(dataset);

        var model = hongKongMark6LotteryResultClusterAnalysis.buildModel(dataset,
                Collections.emptyMap());

        assertNotNull(model);

        var result = hongKongMark6LotteryResultClusterAnalysis.predict(model, JSON_STRING, parameters);

        assertNotNull(result);
        assertEquals(result.count(), 49);

        result.show((int) result.count());
    }

    @PostConstruct
    protected void postConstruct() throws IOException {
        var hkMark6ResultsCsvUri = "file://" + System.getProperty("user.dir") +
                "/example/hk_mark_6_results_20080103-20230520.csv";
        var csvFile = ResourceUtils.getFile(hkMark6ResultsCsvUri);

        amazonS3.createBucket(TEST_S3_BUCKET);
        amazonS3.putObject(TEST_S3_BUCKET, "csv_files/hk_mark_6_results.csv", csvFile);
    }

    @TestConfiguration
    public static class Configuration {

        @Bean
        protected HongKongMark6LotteryResultClusterAnalysis hongKongMark6LotteryResultClusterAnalysis() {
            return new HongKongMark6LotteryResultClusterAnalysis();
        }
    }
}