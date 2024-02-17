package io.openenterprise.daisy.spark.sql.streaming;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Maps;
import io.openenterprise.daisy.AbstractMvelDrivenOperationImpl;
import io.openenterprise.daisy.Invocation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

import static io.openenterprise.daisy.spark.sql.Parameter.DATASET;

public abstract class AbstractSaveStreamingDatasetOperationImpl extends AbstractMvelDrivenOperationImpl<StreamingQuery>
        implements SaveStreamingDatasetOperation {

    protected AbstractSaveStreamingDatasetOperationImpl() {
        super(StreamingQuery.class);
    }

    @Nullable
    @Override
    public StreamingQuery invoke(@Nonnull Map<String, Object> parameters) {
        return withInvocationContext(parameters, (param) -> {
            var dataset = getDataset(param);
            assert Objects.nonNull(dataset);

            if (!dataset.isStreaming()) {
                throw new UnsupportedOperationException("dataset is not streaming");
            }

            var paramCopy = Maps.newHashMap(param);
            paramCopy.put(DATASET.getKey(), dataset);

            return this.eval(paramCopy);
        });
    }

    @Nullable
    protected abstract Dataset<Row> getDataset(@Nonnull Map<String, Object> parameters);

    @Nonnull
    protected String getStreamingQueryVariable(@Nonnull Invocation<?> invocation) {
        return "streamingQuery_" + CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_UNDERSCORE, invocation.getId().toString());
    }
}
