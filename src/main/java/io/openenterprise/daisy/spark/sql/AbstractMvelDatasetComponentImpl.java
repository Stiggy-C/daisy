package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Parameters;
import io.openenterprise.daisy.spark.MvelExpressionService;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Map;

public abstract class AbstractMvelDatasetComponentImpl implements MvelDatasetComponent {

    protected static final Logger LOG = LoggerFactory.getLogger(MvelDatasetComponent.class);

    protected String[] buildDatasetExpressions;

    @Inject
    protected MvelExpressionService mvelExpressionService;

    protected String[] pipelineExpressions;

    protected String[] writeDatasetExpressions;

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public Dataset<Row> buildDataset(@Nonnull Map<String, ?> parameters) {
        var result = evaluateExpressions(buildDatasetExpressions, parameters);

        if (ObjectUtils.isEmpty(result)) {
            return null;
        }

        if (ClassUtils.isAssignable(result.getClass(), Dataset.class)) {
            return (Dataset<Row>) result;
        } else {
            throw new IllegalStateException("Result is not of type, Dataset");
        }
    }

    @Override
    public void pipeline(@Nonnull Map<String, ?> parameters) throws AnalysisException {
        evaluateExpressions(pipelineExpressions, parameters);
    }

    @Override
    public void writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) {
        var sessionId = MapUtils.getString(parameters, Parameters.SESSION_ID.getName());
        mvelExpressionService.addVariable(sessionId, "dataset", dataset);

        evaluateExpressions(writeDatasetExpressions, parameters);
    }

    @Nullable
    protected Object evaluateExpressions(@Nonnull String[] expressions, @Nonnull Map<String, ?> parameters) {
        if (ArrayUtils.isEmpty(expressions)) {
            return null;
        }

        var sessionId = MapUtils.getString(parameters, Parameters.SESSION_ID.getName());
        mvelExpressionService.addVariable(sessionId, "this", this);

        Object result = null;

        for (String expression: expressions) {
            result = mvelExpressionService.evaluate(expression, parameters);
        }

        return result;
    }
}
