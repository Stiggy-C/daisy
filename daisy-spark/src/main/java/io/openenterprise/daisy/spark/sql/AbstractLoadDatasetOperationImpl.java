package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.AbstractMvelDrivenOperationImpl;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@SuppressWarnings("rawtypes")
public abstract class AbstractLoadDatasetOperationImpl extends AbstractMvelDrivenOperationImpl<Dataset>
        implements LoadDatasetOperation {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractLoadDatasetOperationImpl.class);

    public AbstractLoadDatasetOperationImpl() {
        super(Dataset.class);
    }

    @Nullable
    @Override
    public Dataset invoke(@Nonnull Map<String, Object> parameters) {
        return withInvocationContext(parameters, this::load);
    }

    @Nullable
    @Override
    public Dataset<?> load(@Nonnull Map<String, Object> parameters) {
        return super.eval(parameters);
    }
}
