package io.openenterprise.daisy.spark.sql;

import com.google.common.collect.ImmutableMap;
import io.openenterprise.daisy.examples.AbstractTest;
import io.openenterprise.daisy.Parameters;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.annotation.PostConstruct;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@Import(SqlStatementServiceTest.Configuration.class)
class SqlStatementServiceTest extends AbstractTest {

    private static final String SQL = "create table if not exists transactions (id string,\n" +
            "memberId string,\n" +
            "skuId string,\n" +
            "skuCategory string,\n" +
            "skuPrice decimal(6,\n" +
            "2),\n" +
            "createdDateTime timestamp)\n" +
            "using csv\n" +
            "options ('header' = 'true',\n" +
            "'inferSchema' = 'true',\n" +
            "'path' = 's3a://" + TEST_S3_BUCKET + "/csv_files/transactions.csv')\n";

    @Autowired
    protected SqlStatementService sqlStatementService;

    @Test
    void testBuildDataset() {
        var parameters = ImmutableMap.of(Parameters.DATASET_SQL_STATEMENT.getName(), SQL);
        var dataset = sqlStatementService.buildDataset(parameters);

        assertNotNull(dataset);

        assertEquals("id,memberId,skuId,skuCategory,skuPrice,createdDateTime",
                String.join(",", dataset.schema().fieldNames()));
    }

    @PostConstruct
    protected void postConstruct() throws IOException {
        generateExampleTransactions();
    }

    @TestConfiguration
    public static class Configuration {

        @Bean
        public SqlStatementService sqlStatementService() {
            return new SqlStatementService();
        }
    }
}