package io.openenterprise.daisy.examples.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import io.openenterprise.daisy.PlotlySettings;
import io.openenterprise.daisy.spark.ml.AbstractMachineLearningServiceImpl;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import plotly.Trace;
import scala.collection.Seq;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

@Component
@Profile("ml_example")
public class HongKongMark6LotteryResultClusterAnalysis extends AbstractMachineLearningServiceImpl<BisectingKMeansModel, Seq<Trace>, PlotlySettings> {

    public HongKongMark6LotteryResultClusterAnalysis() {
        super(BisectingKMeansModel.class);
    }

    @NotNull
    @Override
    protected BisectingKMeansModel buildModel(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        var bisectingKMeans = new BisectingKMeans();
        bisectingKMeans.setK(5);

        // Build the features column as Spark's KMeans need it
        var transformedDataset = new VectorAssembler().setInputCols(new String[]{"count"})
                .setOutputCol((bisectingKMeans.getFeaturesCol())).transform(dataset);

        return bisectingKMeans.fit(transformedDataset);
    }

    @NotNull
    @Override
    public Dataset<Row> predict(@NotNull BisectingKMeansModel model, @NotNull String jsonString,
                                @Nonnull Map<String, ?> parameters) throws JsonProcessingException {
        var tableOrViewName = getTableName(parameters, getViewName(parameters));

        assert Objects.nonNull(tableOrViewName);

        var tableOrViewExists = isExternalDeltaTable(parameters) || tableOrViewExists(tableOrViewName);

        if (!tableOrViewExists) {
            throw new IllegalStateException();
        }

        var dataset = loadTableOrView(parameters);

        var rootJsonNode = objectMapper.readTree(jsonString);
        var jsonNodes = (rootJsonNode.isArray())? IteratorUtils.toList(rootJsonNode.elements())
                : Lists.newArrayList(new JsonNode[] { rootJsonNode });

        for (var jsonNode : jsonNodes) {
            if (jsonNode.isObject() && !jsonNode.has("count")) {
                var winningNumber = jsonNode.get("winning_number").asInt();
                var row = dataset.select("count").where(col("winning_number")
                        .equalTo(winningNumber)).head();
                var count = row.getLong(row.fieldIndex("count"));

                ((ObjectNode) jsonNode).set("count", new LongNode(count));
            }
        }

        dataset = jsonNodeToDatasetConverter.convert(new ArrayNode(objectMapper.getNodeFactory(), jsonNodes));

        var transformedDataset = new VectorAssembler().setInputCols(new String[]{"count"})
                .setOutputCol(model.getFeaturesCol()).transform(dataset);

        return model.transform(transformedDataset);
    }

    @NotNull
    @Override
    public Dataset<Row> buildDataset(@NotNull Map<String, ?> parameters) {
        // Get csvS3Url from given list of parameters:
        String csvS3Url = parameters.get("csvS3Uri").toString();

        // Build source datasets:
        var csvDataset0 = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(col("Winning Number 1").as("winning_number"))
                .groupBy("winning_number")
                .agg(count("winning_number").as("count"))
                .as("csv0");

        var csvDataset1 = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(col("2").as("winning_number"))
                .groupBy("winning_number")
                .agg(count("winning_number").as("count"))
                .as("csv1");

        var csvDataset2 = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(col("3").as("winning_number"))
                .groupBy("winning_number")
                .agg(count("winning_number").as("count"))
                .as("csv2");

        var csvDataset3 = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(col("4").as("winning_number"))
                .groupBy("winning_number")
                .agg(count("winning_number").as("count"))
                .as("csv3");

        var csvDataset4 = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(col("5").as("winning_number"))
                .groupBy("winning_number")
                .agg(count("winning_number").as("count"))
                .as("csv4");

        var csvDataset5 = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(col("6").as("winning_number"))
                .groupBy("winning_number")
                .agg(count("winning_number").as("count"))
                .as("csv5");

        var csvDataset6 = sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(col("Extra Number ").as("winning_number"))
                .groupBy("winning_number")
                .agg(count("winning_number").as("count"))
                .as("csv6");

        var union = csvDataset0.union(csvDataset1).union(csvDataset2).union(csvDataset3).union(csvDataset4)
                .union(csvDataset5).union(csvDataset6);

        return union.groupBy("winning_number").sum("count").select(col("winning_number"),
                col("sum(count)").as("count")).as("dataset");
    }

    @Override
    public void writeDataset(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        throw new NotImplementedException();
    }

    @Override
    protected Seq<Trace> getPlotData(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        throw new NotImplementedException();
    }

    @NotNull
    @Override
    protected PlotlySettings getPlotSetting(@NotNull Map<String, ?> parameters) {
        throw new NotImplementedException();
    }

    @NotNull
    @Override
    protected File plot(@NotNull String path, @NotNull Seq<Trace> plotData, @NotNull PlotlySettings plotSettings) {
        throw new NotImplementedException();
    }

    @Override
    protected void savePlotToCloudStorage(@NotNull URI pathAsUri, @NotNull File plot) {
        throw new NotImplementedException();
    }

    @NotNull
    @Override
    protected String toPlotJson(@NotNull Seq<Trace> plotData, @NotNull PlotlySettings plotSettings) {
        throw new NotImplementedException();
    }
}
