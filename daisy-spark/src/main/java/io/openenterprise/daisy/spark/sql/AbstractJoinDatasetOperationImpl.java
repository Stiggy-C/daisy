package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.AbstractMvelDrivenOperationImpl;
import org.apache.spark.sql.Dataset;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

@SuppressWarnings("rawtypes")
public abstract class AbstractJoinDatasetOperationImpl extends AbstractMvelDrivenOperationImpl<Dataset>
        implements JoinDatasetOperation  {

    public AbstractJoinDatasetOperationImpl() {
        super(Dataset.class);
    }

    @Nullable
    @Override
    public Dataset invoke(@NotNull Map<String, Object> parameters) {
        return super.withInvocationContext(parameters, this::join);
    }

    @Nullable
    @Override
    public Dataset<?> join(@NotNull Map<String, Object> parameters) {
        return null;
    }
}
