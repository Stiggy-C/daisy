package io.openenterprise.daisy.springframework.boot.autoconfigure.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.openenterprise.daisy.spark.sql.DatasetUtils;
import io.openenterprise.daisy.spark.sql.JdbcUtils;
import io.openenterprise.daisy.spark.sql.execution.datasources.jdbc.MySqlJdbcRelationProvider;
import io.openenterprise.daisy.spark.sql.execution.datasources.jdbc.PostgreSqlJdbcRelationProvider;
import io.openenterprise.daisy.spark.sql.service.DatasetService;
import io.openenterprise.daisy.spark.sql.streaming.service.StreamingDatasetService;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.mvel2.ParserConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

@Configuration("sparkApplicationConfiguration")
@Profile("spark")
public class ApplicationConfiguration
        extends io.openenterprise.daisy.springframework.boot.autoconfigure.ApplicationConfiguration {

    @Autowired
    protected DatasetService datasetService;

    @Autowired
    protected SparkSession sparkSession;

    @Autowired
    protected StreamingDatasetService streamingDatasetService;

    @Bean
    @Qualifier("builtInMvelVariables")
    protected Map<String, Object> builtInMvelVariables(@Nonnull ApplicationContext applicationContext,
                                                       @Nonnull ObjectMapper objectMapper) {
        var variables = super.builtInMvelVariables(applicationContext, objectMapper);
        variables.put("datasetService", datasetService);
        variables.put("spark", sparkSession);
        variables.put("streamingDatasetService", streamingDatasetService);

        return variables;
    }

    @Bean
    protected ParserConfiguration parserConfiguration() {
        var parserConfiguration = super.parserConfiguration();

        // Add Spark functions:
        parserConfiguration.addImport("sql", functions.class);

        // Add Spark related utils:
        parserConfiguration.addImport(DatasetUtils.class);
        parserConfiguration.addImport(JdbcUtils.class);

        return parserConfiguration;
    }

    @Bean
    @Qualifier("streamingCapableFormats")
    protected Set<String> streamingCapableFormats() {
        return Sets.newHashSet("avro", "csv", "delta", "json", "kafka", "orc", "parquet", "text");
    }

    @Bean
    @Qualifier("supportedJdbcBasedFormats")
    protected Set<String> supportedJdbcBasedFormats() {
        return Sets.newHashSet("jdbc", MySqlJdbcRelationProvider.SHORT_NAME,
                PostgreSqlJdbcRelationProvider.SHORT_NAME);
    }

}
