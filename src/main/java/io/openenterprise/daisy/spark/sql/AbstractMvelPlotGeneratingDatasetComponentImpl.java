package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Parameters;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public abstract class AbstractMvelPlotGeneratingDatasetComponentImpl extends AbstractMvelDatasetComponentImpl
        implements MvelPlotGeneratingDatasetComponent {

    protected String[] plotExpressions;

    protected String[] toPlotJsonExpressions;

    @Override
    public void plot(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        var sessionId = MapUtils.getString(parameters, Parameters.SESSION_ID.getName());
        mvelExpressionService.addVariable(sessionId, "dataset", dataset);

        evaluateExpressions(plotExpressions, parameters);
    }

    @Nullable
    @Override
    public String toPlotJson(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        var sessionId = MapUtils.getString(parameters, Parameters.SESSION_ID.getName());
        mvelExpressionService.addVariable(sessionId, "dataset", dataset);

        var result = evaluateExpressions(toPlotJsonExpressions, parameters);

        if (ObjectUtils.isEmpty(result)) {
            return null;
        }

        if (ClassUtils.isAssignable(result.getClass(), String.class)) {
            return (String) result;
        } else {
            throw new IllegalStateException("Result is not of type, String");
        }
    }
}
