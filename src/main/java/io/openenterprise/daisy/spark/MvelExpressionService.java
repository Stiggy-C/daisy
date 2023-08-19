package io.openenterprise.daisy.spark;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.openenterprise.daisy.Parameters;
import io.openenterprise.daisy.mvel2.integration.impl.CachingMapVariableResolverFactory;
import io.openenterprise.daisy.spark.sql.BaseDatasetComponent;
import lombok.Getter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.mvel2.MVEL;
import org.mvel2.ParserConfiguration;
import org.mvel2.ParserContext;
import org.mvel2.compiler.CompiledExpression;
import org.mvel2.compiler.ExpressionCompiler;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.cache.Cache;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Service to evaluate MVEL expression against {@link SparkSession} & implementations of {@link BaseDatasetComponent}. Not to
 * be confused with {@link org.apache.spark.sql.connector.expressions.Expression}
 *
 * Provide ability to store evaluation history in a session cache powered by implementation of {@link Cache} of your
 * choice.
 */
@Service
public class MvelExpressionService {

    protected static final Map<String, CachingMapVariableResolverFactory> SESSIONS_VARIABLE_RESOLVER_FACTORIES =
            Maps.newHashMap();

    @Inject
    protected ApplicationContext applicationContext;

    @Getter
    @Inject
    protected ParserConfiguration parserConfiguration;

    @Inject
    @Named("datasetServicesMap")
    protected Map<String, BaseDatasetComponent> datasetServicesMap;

    @Inject
    @Named("expressionServiceSessionsCache")
    protected Cache<String, Session> sessionsCache;

    @Inject
    protected SparkSession sparkSession;

    public void addVariable(@Nonnull String sessionId, @Nonnull String name, @Nonnull Object value) {
        var cachingMapVariableResolverFactory = MapUtils.getObject(
                SESSIONS_VARIABLE_RESOLVER_FACTORIES, sessionId, createCachingMapVariableResolverFactory());

        cachingMapVariableResolverFactory.getVariables().put(name, value);

        SESSIONS_VARIABLE_RESOLVER_FACTORIES.put(sessionId, cachingMapVariableResolverFactory);
    }

    @Nullable
    public Object evaluate(@Nonnull String expression, @Nonnull Map<String, ?> parameters) {
        return evaluate(expression, parameters, Object.class);
    }

    @Nullable
    public <T> T evaluate(@Nonnull String expression, @Nonnull Map<String, ?> parameters, @Nonnull Class<T> clazz) {
        var sessionId = MapUtils.getString(parameters, Parameters.SESSION_ID.getName());

        var cachingMapVariableResolverFactory = MapUtils.getObject(
                SESSIONS_VARIABLE_RESOLVER_FACTORIES, sessionId, createCachingMapVariableResolverFactory());

        cachingMapVariableResolverFactory.getVariables().put("applicationContext", applicationContext);
        cachingMapVariableResolverFactory.getVariables().put("parameters", parameters);
        cachingMapVariableResolverFactory.getVariables().put("spark", sparkSession);

        SESSIONS_VARIABLE_RESOLVER_FACTORIES.put(sessionId, cachingMapVariableResolverFactory);

        var parserContext = buildParserContext(parserConfiguration, parameters);
        var compiledExpression = parseExpression(expression, parserContext);

        cachingMapVariableResolverFactory.getVariables().put("parserContext", parserContext);

        T result;
        try {
            result = MVEL.executeExpression(compiledExpression, parserContext.getEvaluationContext(),
                    cachingMapVariableResolverFactory, clazz);
        } catch (Exception e) {
            postEvaluate(compiledExpression, parameters, e);

            throw e;
        }

        postEvaluate(compiledExpression, parameters, result);

        return result;
    }

    @Nonnull
    protected ParserContext buildParserContext(@Nonnull ParserConfiguration parserConfiguration,
                                               @Nonnull Map<String, ?> parameters) {
        var thisParserConfiguration = new ParserConfiguration(parserConfiguration.getImports(),
                parserConfiguration.getPackageImports(), parserConfiguration.getInterceptors());

        @SuppressWarnings("unchecked")
        var classNames = (String[]) MapUtils.getObject((Map<String, Object>) parameters,
                Parameters.MVEL_CLASS_IMPORTS.getName(), new String[]{});

        var classSimpleNamesAndClasses =  Arrays.stream(classNames).map(className -> {
            try {
                return ClassUtils.getClass(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toMap(Class::getSimpleName, clazz -> (Object) clazz));

        thisParserConfiguration.addAllImports(classSimpleNamesAndClasses);

        @SuppressWarnings("unchecked")
        var packageNames = (String[]) MapUtils.getObject((Map<String, Object>) parameters,
                Parameters.MVEL_PACKAGE_IMPORTS.getName(), new String[]{});

        for (var packageName: packageNames) {
            thisParserConfiguration.addPackageImport(packageName);
        }

        thisParserConfiguration.addImport("MvelFunction", MvelFunction.class);

        return new ParserContext(thisParserConfiguration);
    }

    protected CachingMapVariableResolverFactory createCachingMapVariableResolverFactory() {
        var variables = Maps.<String, Object>newHashMap(this.datasetServicesMap);

        return new CachingMapVariableResolverFactory(variables);
    }

    @Nonnull
    protected CompiledExpression parseExpression(@Nonnull String expression, @Nonnull ParserContext parserContext) {
        var expressionCompiler = new ExpressionCompiler(expression, parserContext);

        return expressionCompiler.compile();
    }

    protected void postEvaluate(@Nonnull CompiledExpression compiledExpression, @Nonnull Map<String, ?> parameters,
                                @Nonnull Exception exception) {
        upsertSession(compiledExpression, parameters, exception);
    }

    protected void postEvaluate(@Nonnull CompiledExpression expression, @Nonnull Map<String, ?> parameters,
                                @Nullable Object result) {
        var sessionId = MapUtils.getString(parameters, Parameters.SESSION_ID.getName());
        if (ObjectUtils.isNotEmpty(sessionId) && SESSIONS_VARIABLE_RESOLVER_FACTORIES.containsKey(sessionId)) {
            SESSIONS_VARIABLE_RESOLVER_FACTORIES.get(sessionId).getVariables().put("lastEval", result);
        }

        upsertSession(expression, parameters, null);
    }

    protected void upsertSession(@NotNull CompiledExpression expression, @NotNull Map<String, ?> parameters,
                                 @Nullable Exception exception) {
        var sessionId = MapUtils.getString(parameters, Parameters.SESSION_ID.getName());

        if (ObjectUtils.isNotEmpty(sessionId)) {
            upsertSession(sessionId, expression, exception);
        }
    }

    protected void upsertSession(@Nonnull String sessionId, @NotNull CompiledExpression compiledExpression,
                                 @Nullable Exception exception) {
        var evaluationResult = new EvaluationResult().withException(exception)
                .withExpression(compiledExpression.toString()).withSuccess(ObjectUtils.isEmpty(exception));
        var session = sessionsCache.containsKey(sessionId)? sessionsCache.get(sessionId) :
                new Session();
        session.evaluationHistory.add(evaluationResult);

        sessionsCache.put(sessionId, session);
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

    public static class MvelFunction implements Function<Object, Object> {

        protected String expression;

        protected ParserContext parserContext;

        public MvelFunction(@Nonnull String expression, @Nonnull ParserContext parserContext) {
            this.expression = expression;
            this.parserContext = parserContext;
        }

        @Nullable
        @Override
        public Object apply(@Nonnull Object arg) {
            var tempVariableResolverFactory = new CachingMapVariableResolverFactory(Map.of("arg", arg));
            var variableResolverFactory = parserContext.getParserConfiguration()
                    .getVariableFactory(tempVariableResolverFactory);

            return MVEL.eval(expression, variableResolverFactory);
        }
    }

    @Getter
    public static class Session {

        private final List<EvaluationResult> evaluationHistory = Lists.newLinkedList();

    }

}
