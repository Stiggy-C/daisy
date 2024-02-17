package io.openenterprise.daisy.spark.sql;

import com.google.common.base.CaseFormat;
import io.openenterprise.daisy.AbstractMvelDrivenOperationImpl;
import io.openenterprise.daisy.Invocation;
import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.sql.Dataset;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("rawtypes")
public abstract class AbstractLoadDatasetOperationImpl extends AbstractMvelDrivenOperationImpl<Dataset>
        implements LoadDatasetOperation {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractLoadDatasetOperationImpl.class);

    public AbstractLoadDatasetOperationImpl() {
        super(Dataset.class);
    }

    @Override
    protected void verifyParameters(@NotNull Map<String, Object> parameters) {
        assert !ClassUtils.isAssignable(parameters.getClass(), Map.of().getClass().getSuperclass());

        super.verifyParameters(parameters);
    }

    @Nullable
    @Override
    public Dataset invoke(@Nonnull Map<String, Object> parameters) {
        return withInvocationContext(parameters, (param) -> {
            var dataset = this.load(param);
            setDatasetVariableToParameters(parameters);

            return dataset;
        });
    }

    @Nullable
    @Override
    public Dataset<?> load(@NotNull Map<String, Object> parameters) {
        return this.eval(parameters);
    }

    protected String getDatasetVariable(@Nonnull Invocation<?> invocation) {
        return "dataset_" + CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_UNDERSCORE, invocation.getId().toString());
    }

    protected void setDatasetVariableToParameters(@Nonnull Map<String, Object> parameters) {
        var sessionId = this.getSessionId(parameters);
        var invocationContext = this.getInvocationContext(sessionId);

        assert Objects.nonNull(invocationContext);

        var currentInvocation = invocationContext.getCurrentInvocation();

        assert Objects.nonNull(currentInvocation);

        var datasetVariable = getDatasetVariable(currentInvocation);

        parameters.put(Parameter.DATASET_VARIABLE.getKey(), datasetVariable);
    }


}
