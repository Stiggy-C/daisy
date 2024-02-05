package io.openenterprise.daisy.springframework.boot.autoconfigure.spark;

import io.openenterprise.daisy.spark.sql.DatasetUtils;
import io.openenterprise.daisy.spark.sql.JdbcUtils;
import io.openenterprise.daisy.spark.sql.service.DatasetService;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.mvel2.ParserConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

import java.util.Map;

@Configuration("sparkApplicationConfiguration")
@Profile("spark")
public class ApplicationConfiguration
        extends io.openenterprise.daisy.springframework.boot.autoconfigure.ApplicationConfiguration {

    @Autowired
    protected DatasetService datasetService;

    @Autowired
    protected SparkSession sparkSession;

    @Autowired
    @Qualifier("builtInMvelVariables")
    protected Map<String, Object> builtInMvelVariables() {
        var variables = super.builtInMvelVariables();
        variables.put("datasetService", datasetService);
        variables.put("spark", sparkSession);

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
}
