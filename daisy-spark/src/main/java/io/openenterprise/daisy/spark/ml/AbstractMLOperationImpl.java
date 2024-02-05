package io.openenterprise.daisy.spark.ml;

import io.openenterprise.daisy.AbstractOperationImpl;
import io.openenterprise.daisy.spark.sql.DatasetUtils;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractMLOperationImpl<M extends Transformer & MLWritable, T>
        extends AbstractOperationImpl<T> implements MLOperation<M, T> {

    @Nonnull
    protected Dataset<Row> getDataset(@Nonnull Map<String, Object> parameters) {
        var sessionId = getSessionId(parameters);
        var invocationContext = this.getOrCreateInvocationContext(sessionId);

        var dataset = DatasetUtils.getDataset(invocationContext);

        assert Objects.nonNull(dataset);

        return dataset;
    }

}
