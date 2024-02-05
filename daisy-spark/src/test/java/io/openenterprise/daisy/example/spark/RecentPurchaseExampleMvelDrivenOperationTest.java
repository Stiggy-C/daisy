package io.openenterprise.daisy.example.spark;

import com.amazonaws.services.s3.AmazonS3;
import io.openenterprise.daisy.Parameter;
import io.openenterprise.daisy.example.AbstractTest;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.spark.ApplicationConfiguration;
import io.openenterprise.daisy.springframework.core.io.support.YamlPropertySourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@ExtendWith(SpringExtension.class)
@Import({TCacheConfiguration.class, ApplicationConfiguration.class, RecentPurchaseExampleMvelDrivenOperationTest.Configuration.class})
@PropertySource(value = "classpath:recentPurchaseExampleMvelPipeline.yaml", factory = YamlPropertySourceFactory.class)
@TestPropertySource(properties = {"daisy.s3.bucket=daisy", "spring.profiles.active=local_spark,ml_example,spark,tCache"})
class RecentPurchaseExampleMvelDrivenOperationTest extends AbstractTest {

    @Autowired
    protected AmazonS3 amazonS3;

    @Autowired
    @Qualifier("postgresDatasource")
    protected DataSource dataSource;

    @Autowired
    protected RecentPurchaseExampleMvelDrivenOperation recentPurchaseExampleMvelDatasetService;

    @Test
    void test() {
        var parameters = Map.<String, Object>of(
                "csvS3Uri", "s3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv",
                Parameter.MVEL_CLASS_IMPORTS.getKey(), new String[]{},
                Parameter.MVEL_PACKAGE_IMPORTS.getKey(), new String[]{"java.io", "java.net", "java.nio.file",
                        "java.util", "java.util.stream", "scala.collection", "com.amazonaws.services.s3",
                        "com.google.common.collect", "io.openenterprise.daisy", "org.apache.commons.collections4",
                        "org.apache.spark.sql", "plotly", "plotly.layout"},
                Parameter.SESSION_ID.getKey(), UUID.randomUUID(),
                "plotS3Uri", "s3://" + TEST_S3_BUCKET + "/plots/recentPurchaseExampleMvelDatasetService.html"
        );

        Assertions.assertDoesNotThrow(() -> recentPurchaseExampleMvelDatasetService.invoke(parameters));

        var jdbcTemplate = new JdbcTemplate(dataSource);
        var count = jdbcTemplate.queryForObject("select count(*) from members_purchases", Long.class);

        Assertions.assertNotNull(count);
        Assertions.assertTrue(count > 0);
    }

    @TestConfiguration
    protected static class Configuration  {

        @Autowired
        protected Environment environment;

        @Value("${spring.profiles.active}")
        protected String[] profiles;

        @Autowired
        protected MySQLContainer mySQLContainer;

        @Autowired
        protected PostgreSQLContainer postgreSQLContainer;

        @Bean
        protected RecentPurchaseExampleMvelDrivenOperation recentPurchaseExampleMvelDrivenOperation() {
            return new RecentPurchaseExampleMvelDrivenOperation();
        }

        @PostConstruct
        private void postConstruct() {
            var isLocalSpark = Arrays.stream(profiles).anyMatch(profile -> StringUtils.equals("local_spark", profile));

            var mySqlJdbcUrl = isLocalSpark? mySQLContainer.getJdbcUrl() : StringUtils.replace(
                    mySQLContainer.getJdbcUrl(), mySQLContainer.getHost(), "host.testcontainers.internal");
            var postgresJdbcUrl = isLocalSpark? postgreSQLContainer.getJdbcUrl() : StringUtils.replace(
                    postgreSQLContainer.getJdbcUrl(), postgreSQLContainer.getHost(), "host.testcontainers.internal");

            var properties = new Properties();
            properties.setProperty("recentPurchaseExampleMvelPipeline.my-sql-jdbc-password", mySQLContainer.getPassword());
            properties.setProperty("recentPurchaseExampleMvelPipeline.my-sql-jdbc-url", mySqlJdbcUrl);
            properties.setProperty("recentPurchaseExampleMvelPipeline.my-sql-jdbc-user", mySQLContainer.getUsername());
            properties.setProperty("recentPurchaseExampleMvelPipeline.postgres-jdbc-password", postgreSQLContainer.getPassword());
            properties.setProperty("recentPurchaseExampleMvelPipeline.postgres-jdbc-url", postgresJdbcUrl);
            properties.setProperty("recentPurchaseExampleMvelPipeline.postgres-jdbc-user", postgreSQLContainer.getUsername());

            ((ConfigurableEnvironment) environment).getPropertySources()
                    .addLast(new PropertiesPropertySource(RecentPurchaseExampleMvelDrivenOperation.class.getName(), properties));
        }
    }

}