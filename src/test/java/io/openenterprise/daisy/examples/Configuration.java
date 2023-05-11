package io.openenterprise.daisy.examples;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

@TestConfiguration
@Import(io.openenterprise.daisy.Configuration.class)
public class Configuration {

    @Bean
    protected MemberDataGenerator memberDataGenerator() {
        return new MemberDataGenerator();
    }

    @Bean
    protected TransactionsCsvGenerator transactionsCsvGenerator() {
        return new TransactionsCsvGenerator();
    }
}
