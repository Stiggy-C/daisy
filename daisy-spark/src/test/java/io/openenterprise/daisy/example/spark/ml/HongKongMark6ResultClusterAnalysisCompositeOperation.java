package io.openenterprise.daisy.example.spark.ml;

import io.openenterprise.daisy.Parameter;
import io.openenterprise.daisy.CompositeOperation;
import org.apache.commons.collections4.MapUtils;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static io.openenterprise.daisy.Parameter.MVEL_EXPRESSIONS;
import static io.openenterprise.daisy.spark.ml.Parameter.ML_SAVE_MODEL;
import static io.openenterprise.daisy.spark.ml.clustering.Parameter.FEATURES_COLUMN;
import static io.openenterprise.daisy.spark.ml.clustering.Parameter.KMEANS_K;
import static io.openenterprise.daisy.spark.sql.Parameter.DATASET_PATH;

@Component
@Profile("ml_example")
public class HongKongMark6ResultClusterAnalysisCompositeOperation extends CompositeOperation {

    @Nullable
    @Override
    public Void invoke(@NotNull Map<String, Object> parameters) {
        var datasetOperationsBeanNamesOrClasses = List.of("mvelDrivenOperation",
                "kMeansTrainOperation", "mvelDrivenOperation", "kMeansPredictOperation");

        var mvel00 = "dataset0 = spark.read().format(\"csv\").option(\"header\", \"true\")" +
                ".option(\"inferSchema\", \"true\").load(parameters[\"" + DATASET_PATH.getKey() + "\"])" +
                ".select(sql.col(\"Winning Number 1\").as(\"winning_number\"))" +
                ".groupBy(\"winning_number\").agg(sql.count(\"winning_number\").as(\"count\"))" +
                ".as(\"csv0\");";
        var mvel01 = "dataset1 = spark.read().format(\"csv\").option(\"header\", \"true\")" +
                ".option(\"inferSchema\", \"true\").load(parameters[\"" + DATASET_PATH.getKey() + "\"])" +
                ".select(sql.col(\"2\").as(\"winning_number\"))" +
                ".groupBy(\"winning_number\").agg(sql.count(\"winning_number\").as(\"count\"))" +
                ".as(\"csv1\");";
        var mvel02 = "dataset2 = spark.read().format(\"csv\").option(\"header\", \"true\")" +
                ".option(\"inferSchema\", \"true\").load(parameters[\"" + DATASET_PATH.getKey() + "\"])" +
                ".select(sql.col(\"3\").as(\"winning_number\"))\n" +
                ".groupBy(\"winning_number\").agg(sql.count(\"winning_number\").as(\"count\"))" +
                ".as(\"csv2\");";
        var mvel03 = "dataset3 = spark.read().format(\"csv\").option(\"header\", \"true\")" +
                ".option(\"inferSchema\", \"true\").load(parameters[\"" + DATASET_PATH.getKey() + "\"])" +
                ".select(sql.col(\"4\").as(\"winning_number\"))\n" +
                ".groupBy(\"winning_number\").agg(sql.count(\"winning_number\").as(\"count\"))" +
                ".as(\"csv3\");";
        var mvel04 = "dataset4 = spark.read().format(\"csv\").option(\"header\", \"true\")" +
                ".option(\"inferSchema\", \"true\").load(parameters[\"" + DATASET_PATH.getKey() + "\"])" +
                ".select(sql.col(\"5\").as(\"winning_number\"))\n" +
                ".groupBy(\"winning_number\").agg(sql.count(\"winning_number\").as(\"count\"))" +
                ".as(\"csv4\");";
        var mvel05 = "dataset5 = spark.read().format(\"csv\").option(\"header\", \"true\")" +
                ".option(\"inferSchema\", \"true\").load(parameters[\"" + DATASET_PATH.getKey() + "\"])" +
                ".select(sql.col(\"6\").as(\"winning_number\"))\n" +
                ".groupBy(\"winning_number\").agg(sql.count(\"winning_number\").as(\"count\"))" +
                ".as(\"csv5\");";
        var mvel06 = "dataset6 = spark.read().format(\"csv\").option(\"header\", \"true\")" +
                ".option(\"inferSchema\", \"true\").load(parameters[\"" + DATASET_PATH.getKey() + "\"])" +
                ".select(sql.col(\"Extra Number \").as(\"winning_number\"))" +
                ".groupBy(\"winning_number\").agg(sql.count(\"winning_number\").as(\"count\"))" +
                ".as(\"csv6\");";
        var mvel07 = "union = dataset0.union(dataset1).union(dataset2).union(dataset3).union(dataset4)" +
                ".union(dataset5).union(dataset6);";
        var mvel08 = "aggregate = union.groupBy(\"winning_number\").sum(\"count\").select(sql.col(\"winning_number\"), " +
                "sql.col(\"sum(count)\").as(\"count\")).as(\"dataset\"); aggregate";

        var parameters0 = Maps.newHashMap(Map.of(
                DATASET_PATH.getKey(), MapUtils.getString(parameters, DATASET_PATH.getKey()),
                MVEL_EXPRESSIONS.getKey(), mvel00 + mvel01 + mvel02 + mvel03 + mvel04 + mvel05 + mvel06 + mvel07 + mvel08
        ));
        var parameters1 = Maps.newHashMap(Map.<String, Object>of(
                FEATURES_COLUMN.getKey(), new String[] {"count"},
                KMEANS_K.getKey(), 5,
                ML_SAVE_MODEL.getKey(), true));

        var parameters2= Maps.newHashMap(Map.<String, Object>of(
                MVEL_EXPRESSIONS.getKey(), "DatasetUtils.createVectorColumn(aggregate, \"features\", \"count\")"));
        var parameters3= Maps.<String, Object>newHashMap();

        var datasetOperationsParameters= List.of(parameters0, parameters1,
                parameters2, parameters3);

        parameters = Maps.newHashMap(parameters);

        parameters.put(Parameter.DATASET_OPERATIONS.getKey(), datasetOperationsBeanNamesOrClasses);
        parameters.put(Parameter.PARAMETERS.getKey(), datasetOperationsParameters);

        return super.invoke(parameters);
    }
}
