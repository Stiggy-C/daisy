package io.openenterprise.daisy.examples.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.openenterprise.daisy.PlotlySettings;
import io.openenterprise.daisy.spark.ml.AbstractMachineLearningComponentImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import plotly.Trace;
import scala.collection.Seq;

import java.io.File;
import java.net.URI;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Component
@Profile("ml_example")
public class HongKongMark6LotteryResultFrequentPatternMining extends AbstractMachineLearningComponentImpl<FPGrowthModel, Seq<Trace>, PlotlySettings> {

    protected HongKongMark6LotteryResultFrequentPatternMining() {
        super(FPGrowthModel.class);
    }

    @NotNull
    @Override
    protected FPGrowthModel buildModel(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        var fpGrowth = new FPGrowth().
                setMinSupport(1.0/2134); // hk_mark_6_results_20080103-20230520.csv has 2134 rows, want min 1 appearance

        return fpGrowth.fit(dataset);
    }

    @NotNull
    @Override
    public Dataset<Row> predict(@NotNull FPGrowthModel model, @NotNull String jsonString, @NotNull Map<String, ?> parameters)
            throws JsonProcessingException {
        // Get the top 20 pairs:
        var top20Pairs = model.freqItemsets().where(size(col("items")).equalTo(2))
                .orderBy(col("freq").desc()).limit(20);

        // Get the top 20 3-tuples:
        var top20ThreeTuples= model.freqItemsets().where(size(col("items")).equalTo(3))
                .orderBy(col("freq").desc()).limit(20);

        // Get the top 20 4-tuples:
        var top20FourTuples= model.freqItemsets().where(size(col("items")).equalTo(4))
                .orderBy(col("freq").desc()).limit(20);

        // Get the top 20 5-tuples:
        var top20FiveTuples= model.freqItemsets().where(size(col("items")).equalTo(5))
                .orderBy(col("freq").desc()).limit(20);

        return top20Pairs.union(top20ThreeTuples).union(top20FourTuples).union(top20FiveTuples);
    }

    @NotNull
    @Override
    public Dataset<Row> buildDataset(@NotNull Map<String, ?> parameters) {
        // Get csvS3Url from given list of parameters:
        String csvS3Url = parameters.get("csvS3Uri").toString();

        // Build source dataset:
        return sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(array(col("Winning Number 1"), col("2"), col("3"), col("4"),
                        col("5"), col("6"), col("Extra Number ")).as("items"))
                .as("winningNumbers");
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
