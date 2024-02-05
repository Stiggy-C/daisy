package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Operation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Map;

public interface PlotDatasetOperation extends Operation<URI> {

    URI plot(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters);

}
