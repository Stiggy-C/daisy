package io.openenterprise.daisy.examples;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.common.collect.Lists;
import io.openenterprise.daisy.Constants;
import io.openenterprise.daisy.PlotlySettings;
import io.openenterprise.daisy.spark.sql.AbstractPlotGeneratingDatasetServiceImpl;
import org.apache.commons.collections4.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import plotly.*;
import plotly.layout.Grid;
import plotly.layout.Layout;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;

@Component("recentPurchaseExamplePipeline")
@ConditionalOnBean(AmazonS3.class)
@Profile("pipeline_example")
public class RecentPurchaseExampleDatasetService extends AbstractPlotGeneratingDatasetServiceImpl<Seq<Trace>, PlotlySettings> {

    @Inject
    private AmazonS3 amazonS3;

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

    @Nonnull
    @Override
    public Dataset<Row> buildDataset(@Nonnull Map<String, ?> parameters) {
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
    public void writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) {
        // Write back to jdbc:
        dataset.write()
                .format("jdbc")
                .option("url", postgresJdbcUrl)
                .option("dbtable", "recent_purchases")
                .option("user", postgresJdbcUser)
                .option("password", postgresJdbcPassword)
                .save();
    }

    @Override
    protected Seq<Trace> getPlotData(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) {
        // SKU Category bubble chart:
        var subPlot0Dataset = dataset.select("skuCategory").groupBy("skuCategory")
                .agg(count("skuCategory").as("count")).as("subPlot0Dataset");

        var subPlot0Label = subPlot0Dataset.select("skuCategory", "count").collectAsList().stream()
                .map(row -> row.getString(row.fieldIndex("skuCategory"))).collect(Collectors.toList());
        var subPlot0LabelSeq = Sequence.fromStringSeq(JavaConverters.asScalaBuffer(subPlot0Label).toSeq());
        var subPlot0Value = subPlot0Dataset.select("skuCategory", "count").collectAsList().stream()
                .map(row -> row.get(row.fieldIndex("count"))).collect(Collectors.toList());
        var subPlot0ValueSeq = Sequence.fromLongSeq(JavaConverters.asScalaBuffer(subPlot0Value).toSeq());

        var bar0 = new Bar(subPlot0LabelSeq, subPlot0ValueSeq);

        // SKU ID bubble chart:
        var subPlot1Dataset = dataset.select("skuCategory", "skuPrice").groupBy("skuCategory")
                .agg(max("skuPrice").as("maxPrice")).as("subPlot1Dataset");

        var subPlot1Label = subPlot1Dataset.select("skuCategory", "maxPrice").collectAsList().stream()
                .map(row -> row.getString(row.fieldIndex("skuCategory"))).collect(Collectors.toList());
        var subPlot1LabelSeq = Sequence.fromStringSeq(JavaConverters.asScalaBuffer(subPlot1Label).toSeq());
        var subPlot1Value = subPlot1Dataset.select("skuCategory", "maxPrice").collectAsList().stream()
                .map(row -> row.get(row.fieldIndex("maxPrice"))).collect(Collectors.toList());
        var subPlot1ValueSeq = Sequence.fromDoubleSeq(JavaConverters.asScalaBuffer(subPlot1Value).toSeq());

        var bar1 = new Bar(subPlot1LabelSeq, subPlot1ValueSeq);

        var traces = Lists.<Trace>newArrayList(bar0, bar1);

        return JavaConverters.asScalaBuffer(traces).toSeq();
    }

    @Nonnull
    @Override
    protected PlotlySettings getPlotSetting(@Nonnull Map<String, ?> parameters) {
        var plotlySettings = new PlotlySettings();

        var config = new Config().withEditable(false).withResponsive(true);
        var grid = new Grid().withColumns(2).withRows(1);
        var layout = new Layout().withGrid(grid).withTitle(MapUtils.getString(parameters,
                Constants.PLOT_TITLE_PARAMETER_NAME.getValue(), getBeanName()));

        plotlySettings.setConfig(config);
        plotlySettings.setLayout(layout);

        return plotlySettings;
    }

    @Nonnull
    @Override
    protected File plot(@Nonnull String path, @Nonnull Seq<Trace> plotData, @Nonnull PlotlySettings plotlySettings) {
        return Plotly.plot(path, plotData, plotlySettings.getLayout(), plotlySettings.getConfig(),
                plotlySettings.getUseCdn(), plotlySettings.getOpenInBrowser(), plotlySettings.getAddSuffixIfExists());
    }

    @Override
    protected void savePlotToCloudStorage(@NotNull URI uri, @NotNull File plot) {
        var amazonS3Uri = new AmazonS3URI(uri);

        amazonS3.putObject(amazonS3Uri.getBucket(), amazonS3Uri.getKey(), plot);
    }

    @Nonnull
    @Override
    protected String toPlotJson(@Nonnull Seq<Trace> plotData, @Nonnull PlotlySettings plotlySettings) {
        return Plotly.jsonSnippet(plotData, plotlySettings.getLayout(), plotlySettings.getConfig());
    }
}
