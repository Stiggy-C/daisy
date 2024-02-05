package io.openenterprise.daisy.test.spark.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.openenterprise.daisy.spark.sql.DatasetUtils;
import io.openenterprise.daisy.spark.sql.SimpleSaveDatasetOperation;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.spark.ApplicationConfiguration;
import io.openenterprise.daisy.Invocation;
import io.openenterprise.daisy.InvocationContext;
import io.openenterprise.daisy.InvocationContextUtils;
import io.openenterprise.daisy.test.spark.AbstractTest;
import io.openenterprise.daisy.test.spark.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.MySQLContainer;

import javax.inject.Inject;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.UUID;

import static io.openenterprise.daisy.Parameter.SESSION_ID;
import static io.openenterprise.daisy.spark.sql.Parameter.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(SpringExtension.class)
@Import({TCacheConfiguration.class, ApplicationConfiguration.class, Configuration.class,
        SimpleSaveDatasetOperationTest.Configuration.class})
@TestPropertySource(properties = {"spring.profiles.active=local_spark,pipeline_example,spark,tCache"})
class SimpleSaveDatasetOperationTest extends AbstractTest {

    @Inject
    protected MySQLContainer mySQLContainer;

    @Inject
    protected ObjectMapper objectMapper;

    @Inject
    protected SimpleSaveDatasetOperation simpleSaveDatasetOperation;

    @Inject
    protected SparkSession sparkSession;

    @Test
    void testVerifyParametersMissingSessionId() {
        var parameters = Map.<String, Object>of();

        assertThrows(IllegalArgumentException.class, () -> ReflectionTestUtils.invokeMethod(simpleSaveDatasetOperation,
                "verifyParameters", parameters));
    }

    @Test
    void testVerifyParametersOk() {
        var parameters = Map.<String, Object>of(SESSION_ID.getKey(), UUID.randomUUID());

        assertThrows(IllegalArgumentException.class, () -> ReflectionTestUtils.invokeMethod(simpleSaveDatasetOperation,
                "verifyParameters", parameters));
    }

    @Test
    void testInvokeFormatDeltaOk() throws IOException {
        var dataset = DatasetUtils.toDataset(objectMapper.readTree("[{\"col0\": 1}, {\"col0\": 2}]"));
        var invocation = new Invocation<Dataset<Row>>();
        invocation.setResult(dataset);
        var sessionId = UUID.randomUUID();

        var invocationContext = new InvocationContext(sessionId);
        invocationContext.getPastInvocations().add(invocation);

        InvocationContextUtils.getInvocationContextCache().put(sessionId, invocationContext);

        var parameters = Maps.newHashMap(Map.<String, Object>of(
                DATASET_FORMAT.getKey(), DeltaSourceUtils.NAME(),
                DATASET_TABLE.getKey(), getClass().getSimpleName(),
                SESSION_ID.getKey(), sessionId));

        Assertions.assertDoesNotThrow(() -> simpleSaveDatasetOperation.invoke(parameters));
    }

    @Test
    void testInvokeFormatJdbcOk() throws IOException {
        var dataset = DatasetUtils.toDataset(objectMapper.readTree("[{\"col0\": 1}, {\"col0\": 2}]"));
        var invocation = new Invocation<Dataset<Row>>();
        invocation.setResult(dataset);
        var sessionId = UUID.randomUUID();

        var invocationContext = new InvocationContext(sessionId);
        invocationContext.getPastInvocations().add(invocation);

        InvocationContextUtils.getInvocationContextCache().put(sessionId, invocationContext);

        var parameters = Maps.newHashMap(Map.<String, Object>of(
                DATASET_FORMAT.getKey(), "jdbc",
                JDBC_DB_TABLE.getKey(), getClass().getSimpleName(),
                JDBC_DRIVER_CLASS.getKey(), mySQLContainer.getDriverClassName(),
                JDBC_PASSWORD.getKey(), mySQLContainer.getPassword(),
                JDBC_URL.getKey(), mySQLContainer.getJdbcUrl(),
                JDBC_USER.getKey(), mySQLContainer.getUsername(),
                SESSION_ID.getKey(), sessionId));

        Assertions.assertDoesNotThrow(() -> simpleSaveDatasetOperation.invoke(parameters));
    }

    @TestConfiguration
    @ComponentScan(basePackages = {"io.openenterprise.daisy.service", "io.openenterprise.daisy.spark.sql",
            "io.openenterprise.daisy.springframework"},
            excludeFilters = @ComponentScan.Filter(TestConfiguration.class))
    protected static class Configuration {

        @Bean
        protected InvocationContextUtils invocationContextUtils() {
            return new InvocationContextUtils();
        }

        @Bean
        protected SimpleSaveDatasetOperation simpleSaveDatasetOperation() {
            return new SimpleSaveDatasetOperation();
        }

    }
}