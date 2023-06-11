package io.openenterprise.daisy.examples.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.openenterprise.daisy.examples.data.SkuCategory;
import io.openenterprise.daisy.spark.ml.AbstractMachineLearningService;
import lombok.SneakyThrows;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Component
@Profile("ml_example")
public class RecentPurchaseExampleClusterAnalysis extends AbstractMachineLearningService<KMeansModel> {

    @Value("${clusterAnalysisOnRecentPurchaseExample.mySqlJdbcPassword}")
    private String mySqlJdbcPassword;

    @Value("${clusterAnalysisOnRecentPurchaseExample.mySqlJdbcUrl}")
    private String mySqlJdbcUrl;

    @Value("${clusterAnalysisOnRecentPurchaseExample.mySqlJdbcUser}")
    private String mySqlJdbcUser;

    public RecentPurchaseExampleClusterAnalysis() {
        super(KMeansModel.class);
    }

    @Nonnull
    public KMeansModel buildModel(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) {
        var kMeans = new KMeans();
        /*
            Cluster by age groups & sku categories (of most frequent purchases)
            Age groups:
            < 15
            15 - 24
            25 - 34
            35 - 44
            45 - 54
            54 - 64
            65 +
            SKU categories:
            accessories
            decorations
            figures
            kitchen_accessories
            outfits
         */
        kMeans.setK(7 * 5);

        // Build the features column as Spark's KMeans need it
        var transformedDataset = new VectorAssembler().setInputCols(new String[]{"age", "skuCategory"})
                .setOutputCol((kMeans.getFeaturesCol())).transform(dataset);

        return kMeans.fit(transformedDataset);
    }


    @Nonnull
    @Override
    public Dataset<Row> predict(@Nonnull KMeansModel model, @Nonnull String jsonString, @Nonnull Map<String, ?> parameters) throws JsonProcessingException {
        var dataset = super.convertJsonStringToDataset(jsonString);
        var transformedDataset = new VectorAssembler().setInputCols(new String[]{"age", "skuCategory"})
                .setOutputCol(model.getFeaturesCol()).transform(dataset);

        return model.transform(transformedDataset);
    }


    @Override
    @Nonnull
    public Dataset<Row> buildDataset(@Nonnull Map<String, ?> parameters) {
        // Get csvS3Url from given list of parameters:
        String csvS3Url = parameters.get("csvS3Uri").toString();

        // Build source datasets:
        // Get count of each skuCategory of each member:
        var csvDataset0 = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .groupBy("memberId", "skuCategory")
                .agg(count("skuCategory").as("count"))
                .as("csv0");
        // Get max count of each member:
        var csvDataset1 = csvDataset0
                .select("memberId", "count")
                .groupBy("memberId", "count")
                .agg(max("count"))
                .select(col("memberId").as("memberId1"), col("count").as("max"))
                .as("csv1");

        // Join by csv0.memberId = csv1.memberId & csv0.count = csv1.max to find favourite skuCategory:
        var csvJoinColumn0 = csvDataset0.col("memberId").equalTo(csvDataset1.col("memberId1"));
        var csvJoinColumn1 = csvDataset0.col("count").equalTo(csvDataset1.col("max"));

        // Aggregated dataset should only have member and its most frequent purchased skuCategory and its count
        var csvDataset = csvDataset0.join(csvDataset1, csvJoinColumn0.and(csvJoinColumn1))
                .select("memberId", "skuCategory", "count");

        // Transform categories to their corresponding index for clustering:
        var transformedCsvDataset = csvDataset.select(new Column("memberId"),
                when(new Column("skuCategory").equalTo(SkuCategory.ACCESSORIES.name()), 0)
                        .when(new Column("skuCategory").equalTo(SkuCategory.DECORATIONS.name()), 1)
                        .when(new Column("skuCategory").equalTo(SkuCategory.FIGURES.name()), 2)
                        .when(new Column("skuCategory").equalTo(SkuCategory.KITCHEN_ACCESSORIES.name()), 3)
                        .when(new Column("skuCategory").equalTo(SkuCategory.OUTFITS.name()), 4)
                        .as("skuCategory"));

        var jdbcDataset = sparkSession.read().format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", mySqlJdbcUrl)
                .option("password", mySqlJdbcPassword)
                .option("user", mySqlJdbcUser)
                .option("query", "select m.id, m.age, m.gender, m.tier from member m")
                .load()
                .as("jdbc");

        // Join source datasets to build a frequent recent purchases' dataset for clustering analysis:
        var joinColumn = jdbcDataset.col("id").equalTo(transformedCsvDataset.col("memberId"));

        return jdbcDataset.join(transformedCsvDataset, joinColumn)
                .select("memberId", "age", "gender", "tier", "skuCategory");
    }
}
