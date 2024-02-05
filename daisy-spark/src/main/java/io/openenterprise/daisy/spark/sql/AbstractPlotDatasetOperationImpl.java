package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.AbstractOperationImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractPlotDatasetOperationImpl extends AbstractOperationImpl<URI> implements PlotDatasetOperation {

    @Nullable
    @Override
    public URI invoke(@NotNull Map<String, Object> parameters) {
        withInvocationContext(parameters, (params) ->  {
            var dataset = getDataset(params);
            if (Objects.isNull(dataset)) {
                throw new IllegalStateException("dataset can not be null");
            }

            plot(dataset, params);

            return null;
        });

        return null;
    }

    @Nullable
    protected abstract Dataset<Row> getDataset(@NotNull Map<String, Object> parameters);
}
