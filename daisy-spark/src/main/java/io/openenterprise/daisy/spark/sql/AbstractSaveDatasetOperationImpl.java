package io.openenterprise.daisy.spark.sql;

import com.google.common.collect.Maps;
import io.openenterprise.daisy.AbstractMvelDrivenOperationImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

import static io.openenterprise.daisy.spark.sql.Parameter.DATASET;

public abstract class AbstractSaveDatasetOperationImpl extends AbstractMvelDrivenOperationImpl<Void>
        implements SaveDatasetOperation {

    public AbstractSaveDatasetOperationImpl() {
        super(Void.class);
    }

    @Nullable
    @Override
    public Void invoke(@Nonnull Map<String, Object> parameters) {
        withInvocationContext(parameters, (param) -> {
            var dataset = getDataset(param);
            var paramCopy = Maps.newHashMap(param);
            paramCopy.put(DATASET.getKey(), dataset);

            assert Objects.nonNull(dataset);

            this.eval(paramCopy);

            return null;
        });

        return null;
    }

    protected abstract Dataset<Row> getDataset(@Nonnull Map<String, Object> parameters);
}
