package io.openenterprise.daisy.spark;

import com.google.common.collect.Lists;
import io.openenterprise.daisy.Constants;
import io.openenterprise.daisy.spark.sql.BaseDatasetService;
import lombok.Getter;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.cache.Cache;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service to evaluate SpEL expression against {@link SparkSession} & implementations of {@link BaseDatasetService}. Not to
 * be confused with {@link org.apache.spark.sql.connector.expressions.Expression}
 *
 * Provide ability to store evaluation history in a session cache powered by implementation of {@link Cache} of your
 * choice.
 */
@Service
public class ExpressionService {

    @Inject
    @Named("expressionServiceSessionsCache")
    protected Cache<String, Session> sessionsCache;

    @Inject
    protected ExpressionParser expressionParser;

    @Inject
    @Named("datasetServicesMap")
    protected Map<String, BaseDatasetService> datasetServicesMap;

    @Inject
    protected SparkSession sparkSession;

    @Nonnull
    @SuppressWarnings("unchecked")
    public EvaluationContext buildEvaluationContext(@Nonnull Map<String, ?> parameters) {
        var evaluationContext = new StandardEvaluationContext();

        evaluationContext.setVariable("parameters", parameters);
        evaluationContext.setVariable("spark", sparkSession);

        evaluationContext.setVariables(datasetServicesMap.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue)));

        return evaluationContext;
    }

    @Nonnull
    public Expression parseExpression(@Nonnull String expressionString) {
        return expressionParser.parseExpression(expressionString);
    }

    @Nullable
    public Object evaluate(@Nonnull Expression expression, @Nonnull EvaluationContext evaluationContext) {
        return evaluate(expression, evaluationContext, Object.class);
    }

    @Nullable
    public <T> T evaluate(@Nonnull Expression expression, @Nonnull EvaluationContext evaluationContext,
                          @Nonnull Class<T> clazz) {
        T result;
        try {
            result = expression.getValue(evaluationContext, clazz);
        } catch (Exception e) {
            postEvaluate(expression, evaluationContext, e);

            throw e;
        }

        postEvaluate(expression, evaluationContext, result);

        return result;
    }

    protected void postEvaluate(@Nonnull Expression expression, @Nonnull EvaluationContext evaluationContext,
                                @Nonnull Exception exception) {
        upsertSession(expression, evaluationContext, exception);
    }

    protected void postEvaluate(@Nonnull Expression expression, @Nonnull EvaluationContext evaluationContext,
                                @Nullable Object result) {
        if (ObjectUtils.isNotEmpty(result) && ClassUtils.isAssignable(result.getClass(), Dataset.class)) {
            var viewName = expressionParser.parseExpression("#parameters['" +
                            Constants.VIEW_NAME_PARAMETER_NAME.getValue() + "']")
                    .getValue(evaluationContext, String.class);

            if (ObjectUtils.isNotEmpty(viewName)) {
                ((Dataset<?>) result).createOrReplaceGlobalTempView(viewName);
            }
        }

        upsertSession(expression, evaluationContext, null);
    }

    protected void upsertSession(@NotNull Expression expression, @NotNull EvaluationContext evaluationContext,
                                 @Nullable Exception exception) {
        var sessionId = expressionParser.parseExpression("#parameters['" +
                        Constants.SESSION_ID_PARAMETER_NAME.getValue() + "']")
                .getValue(evaluationContext, String.class);

        if (ObjectUtils.isNotEmpty(sessionId)) {
            upsertSession(sessionId, expression, exception);
        }
    }

    protected void upsertSession(@Nonnull String sessionId, @NotNull Expression expression,
                                 @Nullable Exception exception) {
        var evaluationResult = new EvaluationResult().withException(exception)
                .withExpression(expression.getExpressionString()).withSuccess(ObjectUtils.isEmpty(exception));
        var session = sessionsCache.containsKey(sessionId)? sessionsCache.get(sessionId) :
                new Session();
        session.evaluationHistory.add(evaluationResult);

        sessionsCache.put(sessionId, session);
    }

    @Getter
    public static class Session {

        private final List<EvaluationResult> evaluationHistory = Lists.newLinkedList();

    }

    @Getter
    public static class EvaluationResult {

        @Nullable
        private Exception exception;

        private String expression;

        private boolean success;

        @Nonnull
        protected EvaluationResult withException(@Nullable Exception e) {
            this.exception = e;

            return this;
        }

        @Nonnull
        protected EvaluationResult withExpression(@Nonnull String expression) {
            this.expression = expression;

            return this;
        }

        @Nonnull
        protected EvaluationResult withSuccess(boolean success) {
            this.success = success;

            return this;
        }
    }
}
