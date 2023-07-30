package io.openenterprise.daisy.examples;

import io.openenterprise.daisy.spark.sql.AbstractStreamingDatasetServiceImpl;
import io.openenterprise.daisy.spark.api.java.function.ForeachJdbcWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import scala.collection.immutable.Map.Map4;

import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 *
 */
@Component("recentPurchaseExampleStreamingPipeline")
@Profile("pipeline_example")
public class RecentPurchaseExampleStreamingDatasetService extends AbstractStreamingDatasetServiceImpl {

    /*
     * Can not be a children (sub-path) of csvFolderS3Uri!!
     */
    @Value("${recentPurchaseExampleStreamingPipeline.csvArchiveFolderS3Uri}")
    private String csvArchiveFolderS3Uri;

    @Value("${recentPurchaseExampleStreamingPipeline.csvFolderS3Uri}")
    private String csvFolderS3Uri;

    @Value("${recentPurchaseExampleStreamingPipeline.mySqlJdbcPassword}")
    private String mySqlJdbcPassword;

    @Value("${recentPurchaseExampleStreamingPipeline.mySqlJdbcUrl}")
    private String mySqlJdbcUrl;

    @Value("${recentPurchaseExampleStreamingPipeline.mySqlJdbcUser}")
    private String mySqlJdbcUser;

    @Value("${recentPurchaseExampleStreamingPipeline.postgresJdbcPassword}")
    private String postgresJdbcPassword;

    @Value("${recentPurchaseExampleStreamingPipeline.postgresJdbcUrl}")
    private String postgresJdbcUrl;

    @Value("${recentPurchaseExampleStreamingPipeline.postgresJdbcUser}")
    private String postgresJdbcUser;

    @Value("${recentPurchaseExampleStreamingPipeline.sparkCheckpointLocation}")
    private String sparkCheckpointLocation;

    @NotNull
    @Override
    public Dataset<Row> buildDataset(@NotNull Map<String, ?> parameters) {
        // Build source datasets:
        var csvDataset = sparkSession.readStream().format("csv")
                .option("cleanSource", "archive")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sourceArchiveDir", csvArchiveFolderS3Uri)
                .load(csvFolderS3Uri)
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

        // Join source datasets to build a recent purchases' dataset for clustering analysis:
        var joinColumn = jdbcDataset.col("id").equalTo(csvDataset.col("memberId"));

        return csvDataset.join(jdbcDataset, joinColumn);
    }

    @Override
    public StreamingQuery writeDataset(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) throws TimeoutException {
        var scalaImmutableMap = new Map4<>(JDBCOptions.JDBC_TABLE_NAME(), "recent_purchases",
                JDBCOptions.JDBC_URL(), postgresJdbcUrl,
                "user", postgresJdbcUser,
                "password", postgresJdbcPassword);

        var jdbcOptionsInWrite = new JdbcOptionsInWrite(scalaImmutableMap);

        var foreachJdbcWriter = new ForeachJdbcWriter();
        foreachJdbcWriter.setJdbcOptionsInWrite(jdbcOptionsInWrite);
        foreachJdbcWriter.setPassword(postgresJdbcPassword);
        foreachJdbcWriter.setUsername(postgresJdbcUser);

        // Use default OutputMode as WriteDatasetToJdbcFunction can handle upsert
        return dataset.writeStream()
                .foreach(foreachJdbcWriter)
                .option("checkpointLocation", sparkCheckpointLocation)
                .trigger(Trigger.ProcessingTime(10000))
                .start();
    }
}
