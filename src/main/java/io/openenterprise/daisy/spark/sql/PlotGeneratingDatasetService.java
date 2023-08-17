package io.openenterprise.daisy.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface PlotGeneratingDatasetService extends DatasetService {

    void plot(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);

    @Nullable
    String toPlotJson(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);
}

interface MvelPlotGeneratingDatasetService extends PlotGeneratingDatasetService {}