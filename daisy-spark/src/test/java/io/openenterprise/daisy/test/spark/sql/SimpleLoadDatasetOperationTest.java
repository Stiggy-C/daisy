package io.openenterprise.daisy.test.spark.sql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.openenterprise.daisy.spark.sql.SimpleLoadDatasetOperation;
import io.openenterprise.daisy.spark.sql.service.DatasetService;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.spark.ApplicationConfiguration;
import io.openenterprise.daisy.test.AbstractTest;
import io.openenterprise.daisy.test.MemberDataGenerator;

import io.openenterprise.daisy.test.spark.Configuration;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.MySQLContainer;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.UUID;

import static io.openenterprise.daisy.Parameter.SESSION_ID;
import static io.openenterprise.daisy.spark.sql.Parameter.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@Import({TCacheConfiguration.class, ApplicationConfiguration.class, Configuration.class})
@TestPropertySource(properties = {"spring.profiles.active=local_spark,pipeline_example,spark,tCache"})
class SimpleLoadDatasetOperationTest extends AbstractTest {

    @Inject
    protected MySQLContainer mySQLContainer;

    @Inject
    protected SimpleLoadDatasetOperation simpleLoadDatasetOperation;

    @Inject
    protected SparkSession sparkSession;

    @Test
    public void testVerifyDatasetAllPresent() {
        var dataset = sparkSession.createDataset(Lists.newArrayList("string"), Encoders.STRING());
        var parameters = Map.<String, Object>of(
                DATASET.getKey(), dataset,
                DATASET_FORMAT.getKey(), "jdbc",
                DATASET_TABLE.getKey(), "table",
                DATASET_VIEW.getKey(), "view",
                SESSION_ID.getKey(), UUID.randomUUID());

        assertThrows(IllegalArgumentException.class, () -> simpleLoadDatasetOperation.verifyParameters(parameters));
    }

    @Test
    void testVerifyParametersMissingSessionId() {
        var parameters = Map.<String, Object>of();

        assertThrows(IllegalArgumentException.class, () -> simpleLoadDatasetOperation.verifyParameters(parameters));
    }

    @Test
    public void testVerifyDatasetNonePresent() {
        var parameters = Map.<String, Object>of(SESSION_ID.getKey(), UUID.randomUUID());

        assertThrows(IllegalArgumentException.class, () -> simpleLoadDatasetOperation.verifyParameters(parameters));
    }

    @SneakyThrows
    @Test
    public void testVerifyDatasetFormatJdbcOk() {
        var parameters = Maps.newHashMap(Map.<String, Object>of(
                DATASET_FORMAT.getKey(), "jdbc",
                JDBC_DB_TABLE.getKey(), MemberDataGenerator.DB_TABLE,
                JDBC_DRIVER_CLASS.getKey(), mySQLContainer.getDriverClassName(),
                JDBC_PASSWORD.getKey(), mySQLContainer.getPassword(),
                JDBC_URL.getKey(), mySQLContainer.getJdbcUrl(),
                JDBC_USER.getKey(), mySQLContainer.getUsername(),
                SESSION_ID.getKey(), UUID.randomUUID()));
        invokeOperationInit(simpleLoadDatasetOperation, parameters);

        assertDoesNotThrow(() -> simpleLoadDatasetOperation.verifyParameters(parameters));
    }

    @SneakyThrows
    @Test
    public void testVerifyDatasetFormatNonJdbcOk() {
        var parameters = Maps.newHashMap(Map.<String, Object>of(
                DATASET_FORMAT.getKey(), "json",
                DATASET_PATH.getKey(), "path",
                SESSION_ID.getKey(), UUID.randomUUID()));
        invokeOperationInit(simpleLoadDatasetOperation, parameters);

        assertDoesNotThrow(() -> simpleLoadDatasetOperation.verifyParameters(parameters));
    }

    @SneakyThrows
    @Test
    public void testVerifyDatasetTableOk() {
        var parameters = Maps.newHashMap(Map.<String, Object>of(
                DATASET_TABLE.getKey(), "table",
                SESSION_ID.getKey(), UUID.randomUUID()));
        invokeOperationInit(simpleLoadDatasetOperation, parameters);

        assertDoesNotThrow(() -> simpleLoadDatasetOperation.verifyParameters(parameters));
    }

    @SneakyThrows
    @Test
    public void testVerifyDatasetViewOk() {
        var parameters = Maps.newHashMap(Map.<String, Object>of(
                DATASET_VIEW.getKey(), "view",
                SESSION_ID.getKey(), UUID.randomUUID()));
        invokeOperationInit(simpleLoadDatasetOperation, parameters);

        assertDoesNotThrow(() -> simpleLoadDatasetOperation.verifyParameters(parameters));
    }

    @Test
    public void testLoadDatasetFormatJdbc() {
        var parameters = Maps.newHashMap(Map.<String, Object>of(
                DATASET_FORMAT.getKey(), "jdbc",
                JDBC_DB_TABLE.getKey(), MemberDataGenerator.DB_TABLE,
                JDBC_DRIVER_CLASS.getKey(), mySQLContainer.getDriverClassName(),
                JDBC_PASSWORD.getKey(), mySQLContainer.getPassword(),
                JDBC_URL.getKey(), mySQLContainer.getJdbcUrl(),
                JDBC_USER.getKey(), mySQLContainer.getUsername(),
                SESSION_ID.getKey(), UUID.randomUUID()));

        var dataset = simpleLoadDatasetOperation.invoke(parameters);

        assertNotNull(dataset);
        assertTrue(dataset.count() > 0);
    }

    private static void invokeOperationInit(@Nonnull SimpleLoadDatasetOperation simpleLoadDatasetOperation,
                                            @Nonnull Map<String, Object> parameters) throws InvocationTargetException,
            IllegalAccessException {
        var method = MethodUtils.getMatchingMethod(simpleLoadDatasetOperation.getClass(), "init", Map.class);
        method.setAccessible(true);

        method.invoke(simpleLoadDatasetOperation, parameters);
    }
}