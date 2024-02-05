package io.openenterprise.daisy.service;

import org.mvel2.ParserContext;
import org.mvel2.compiler.ExecutableStatement;
import org.mvel2.integration.VariableResolverFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Consumer;

public interface MvelExpressionEvaluationService {

    @Nonnull
    ParserContext buildParserContext(@Nonnull Set<Class<?>> classImports, Set<String> packageImports);

    @Nonnull
    ExecutableStatement compileExpression(@Nonnull String expression, @Nonnull ParserContext parserContext);

    @Nullable
    Object evaluate(@Nonnull ExecutableStatement compiledExpression, @Nonnull VariableResolverFactory variableResolverFactory);

    @Nullable
    <T> T evaluate(@Nonnull ExecutableStatement compiledExpression, @Nonnull VariableResolverFactory variableResolverFactory,
                   @Nonnull Class<T> resultClass);

    @Nullable
    Object evaluate(@Nonnull ExecutableStatement compiledExpression, @Nonnull VariableResolverFactory variableResolverFactory,
                    @Nullable Consumer<Object> postEvalCallback, @Nullable Consumer<Exception> postEvalExceptionThrownCallback);

    @Nullable
    <T> T evaluate(@Nonnull ExecutableStatement compiledExpression, @Nonnull VariableResolverFactory variableResolverFactory,
                   @Nonnull Class<T> resultClass, @Nullable Consumer<T> postEvalCallback,
                   @Nullable Consumer<Exception> postEvalExceptionThrownCallback);
}
