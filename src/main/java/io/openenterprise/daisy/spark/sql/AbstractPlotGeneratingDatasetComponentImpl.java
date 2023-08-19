package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Parameters;
import io.openenterprise.daisy.PlotSettings;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;

public abstract class AbstractPlotGeneratingDatasetComponentImpl<PD, PS extends PlotSettings>
        extends AbstractDatasetComponentImpl implements PlotGeneratingDatasetComponent {

    @Override
    public void plot(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        var path = getPlotPath(parameters);
        var pathAsUri = URI.create(path);
        var isCloudStorage = StringUtils.equalsIgnoreCase("s3", pathAsUri.getScheme());

        if (isCloudStorage) {
            try {
                var tempDirectory = Files.createTempDirectory(getBeanName());
                var tempFile = Files.createFile(tempDirectory.resolve("plot.html"));

                path = tempFile.toAbsolutePath().toString();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        var plotData = getPlotData(dataset, parameters);
        var plotSetting = getPlotSetting(parameters);

        var plot = plot(path, plotData, plotSetting);

        if (isCloudStorage) {
           savePlotToCloudStorage(pathAsUri, plot);
        }
    }

    @NotNull
    @Override
    public String toPlotJson(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        var plotData = getPlotData(dataset, parameters);
        var plotSetting = getPlotSetting(parameters);

        return toPlotJson(plotData, plotSetting);
    }

    protected abstract PD getPlotData(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);

    @Nonnull
    protected String getPlotPath(@Nonnull Map<String, ?> parameters) {
        var path = MapUtils.getString(parameters, Parameters.PLOT_PATH.getName());

        if (StringUtils.isEmpty(path)) {
            throw new IllegalArgumentException(Parameters.PLOT_PATH + " is empty");
        }

        return path;
    }

    @Nonnull
    protected abstract PS getPlotSetting(@Nonnull Map<String, ?> parameters);

    @Nonnull
    protected abstract File plot(@Nonnull String path, @Nonnull PD plotData, @Nonnull PS plotSettings);

    protected abstract void savePlotToCloudStorage(@Nonnull URI pathAsUri, @Nonnull File plot);

    @Nonnull
    protected abstract  String toPlotJson(@Nonnull PD plotData, @Nonnull PS plotSettings);

}
