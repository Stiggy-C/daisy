package io.openenterprise.daisy.examples;

import io.openenterprise.daisy.spark.AbstractPipeline;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.Map;

@Component("recentPurchaseExamplePipeline")
@Profile("example")
public class RecentPurchaseExamplePipeline extends AbstractPipeline {

    @Value("${recentPurchaseExamplePipeline.mySqlJdbcPassword}")
    private String mySqlJdbcPassword;

    @Value("${recentPurchaseExamplePipeline.mySqlJdbcUrl}")
    private String mySqlJdbcUrl;

    @Value("${recentPurchaseExamplePipeline.mySqlJdbcUser}")
    private String mySqlJdbcUser;

    @Value("${recentPurchaseExamplePipeline.postgresJdbcPassword}")
    private String postgresJdbcPassword;

    @Value("${recentPurchaseExamplePipeline.postgresJdbcUrl}")
    private String postgresJdbcUrl;

    @Value("${recentPurchaseExamplePipeline.postgresJdbcUser}")
    private String postgresJdbcUser;

    @NotNull
    @Override
    protected Dataset<Row> buildDataset(@Nonnull Map<String, ?> parameters) {
        // Get csvS3Url from given list of parameters:
        String csvS3Uri = parameters.get("csvS3Uri").toString();

        // Build source datasets:
        var csvDataset = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Uri)
                .select("memberId", "skuId", "skuCategory", "skuPrice", "createdDateTime")
                .as("csv");
        var jdbcDataset = sparkSession.read().format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", mySqlJdbcUrl)
                .option("password", mySqlJdbcPassword)
                .option("user", mySqlJdbcUser)
                .option("query", "select m.id, m.age, m.gender, m.tier from member m")
                .load()
                .as("jdbc");

        // Join source datasets to build a recent purchases:
        return jdbcDataset.join(csvDataset, jdbcDataset.col("id").equalTo(csvDataset.col("memberId")));
    }

    @Override
    protected void writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) {
        // Write back to jdbc:
        dataset.write()
                .format("jdbc")
                .option("url", postgresJdbcUrl)
                .option("dbtable", "recent_purchases")
                .option("user", postgresJdbcUser)
                .option("password", postgresJdbcPassword)
                .save();
    }
}
