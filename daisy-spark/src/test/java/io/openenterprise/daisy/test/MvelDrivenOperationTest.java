package io.openenterprise.daisy.test;

import com.google.common.collect.Maps;
import io.openenterprise.daisy.MvelDrivenOperation;
import io.openenterprise.daisy.MvelDrivenOperationImpl;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.spark.ApplicationConfiguration;
import io.openenterprise.daisy.test.spark.Configuration;
import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.MySQLContainer;

import javax.inject.Inject;
import java.util.Map;

import static io.openenterprise.daisy.Parameter.MVEL_CLASS_IMPORTS;
import static io.openenterprise.daisy.Parameter.MVEL_EXPRESSIONS;
import static io.openenterprise.daisy.spark.sql.Parameter.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "io.openenterprise.daisy.service")
@Import({TCacheConfiguration.class, ApplicationConfiguration.class, Configuration.class,
        MvelDrivenOperationTest.Configuration.class})
@TestPropertySource(properties = {"spring.profiles.active=local_spark,pipeline_example,spark,tCache"})
public class MvelDrivenOperationTest extends AbstractTest {

    @Inject
    protected MvelDrivenOperationImpl mvelDrivenDatasetOperation;

    @Inject
    protected MySQLContainer mySQLContainer;

    @Test
    void testInvoke() {
        var mvelExpression = "spark.read().format(\"jdbc\")" +
                ".option(\"url\", parameters[Parameter.JDBC_URL.key])" +
                ".option(\"dbtable\", parameters[Parameter.JDBC_DB_TABLE.key])" +
                ".option(\"user\", parameters[Parameter.JDBC_USER.key])" +
                ".option(\"password\", parameters[Parameter.JDBC_PASSWORD.key])" +
                ".load();";
        var parameters = Maps.newHashMap(Map.<String, Object>of(
                JDBC_DB_TABLE.getKey(), MemberDataGenerator.DB_TABLE,
                JDBC_DRIVER_CLASS.getKey(), mySQLContainer.getDriverClassName(),
                JDBC_PASSWORD.getKey(), mySQLContainer.getPassword(),
                JDBC_URL.getKey(), mySQLContainer.getJdbcUrl(),
                JDBC_USER.getKey(), mySQLContainer.getUsername(),
                MVEL_CLASS_IMPORTS.getKey(), new String[]{"io.openenterprise.daisy.spark.sql.Parameter"},
                MVEL_EXPRESSIONS.getKey(), mvelExpression
        ));

        var result = mvelDrivenDatasetOperation.invoke(parameters);

        assertNotNull(result);
        assertTrue(ClassUtils.isAssignable(result.getClass(), Dataset.class));
    }

    @TestConfiguration
    public static class Configuration {

        @Bean
        protected MvelDrivenOperation<Object> mvelDrivenOperation() {
            return new MvelDrivenOperationImpl();
        }
    }
}