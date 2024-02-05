package io.openenterprise.daisy.service;

import org.jetbrains.annotations.NotNull;
import org.mvel2.MVEL;
import org.mvel2.ParserConfiguration;
import org.mvel2.ParserContext;
import org.mvel2.compiler.ExecutableStatement;
import org.mvel2.integration.VariableResolverFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

@Service
public class MvelExpressionEvaluationServiceImpl implements MvelExpressionEvaluationService {

    /**
     * Default org.mvel2.ParserContext
     */
    @Inject
    protected ParserConfiguration parserConfiguration;

    @Nonnull
    @Override
    public ParserContext buildParserContext(@Nonnull Set<Class<?>> classImports, Set<String> packageImports) {
        var parserContext = new ParserContext(parserConfiguration);

        classImports.forEach(parserContext::addImport);
        packageImports.forEach(parserContext::addPackageImport);

        return parserContext;
    }

    @Nonnull
    @Override
    public ExecutableStatement compileExpression(@NotNull String expression, @NotNull ParserContext parserContext) {
        return (ExecutableStatement) MVEL.compileExpression(expression, parserContext);
    }

    @Nullable
    @Override
    public Object evaluate(@Nonnull ExecutableStatement compiledExpression,
                           @Nonnull VariableResolverFactory variableResolverFactory) {
        return this.evaluate(compiledExpression, variableResolverFactory, Object.class);
    }

    @Nullable
    @Override
    public <T> T evaluate(@Nonnull ExecutableStatement compiledExpression,
                          @Nonnull VariableResolverFactory variableResolverFactory,
                          @Nonnull Class<T> resultClass) {
        return this.evaluate(compiledExpression, variableResolverFactory, resultClass, null,
                null);
    }

    @Nullable
    public Object evaluate(@Nonnull ExecutableStatement compiledExpression,
                           @Nonnull VariableResolverFactory variableResolverFactory,
                           @Nullable Consumer<Object> postEvalCallback,
                           @Nullable Consumer<Exception> postEvalExceptionThrownCallback) {
        return this.evaluate(compiledExpression, variableResolverFactory, Object.class, null,
                null);
    }

    @Nullable
    @Override
    public <T> T evaluate(@Nonnull ExecutableStatement compiledExpression,
                          @Nonnull VariableResolverFactory variableResolverFactory,
                          @Nonnull Class<T> resultClass, @Nullable Consumer<T> postEvalCallback,
                          @Nullable Consumer<Exception> postEvalExceptionThrownCallback) {
        T result;

        try {
            result = resultClass.cast(MVEL.executeExpression(compiledExpression,
                    variableResolverFactory));
        } catch (Exception e) {
            if (Objects.nonNull(postEvalExceptionThrownCallback)) {
                postEvalExceptionThrownCallback.accept(e);
            }

            throw e;
        }

        try {
            return result;
        } finally {
            if (Objects.nonNull(postEvalCallback)) {
                postEvalCallback.accept(result);
            }
        }
    }
}
