package io.openenterprise.daisy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.openenterprise.daisy.mvel2.integration.impl.CachingMapVariableResolverFactory;
import io.openenterprise.daisy.service.MvelExpressionEvaluationService;
import lombok.Getter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.openenterprise.daisy.Parameter.*;

public abstract class AbstractMvelDrivenOperationImpl<T> extends AbstractOperationImpl<T> implements MvelDrivenOperation<T> {

    protected Class<T> resultClass;

    protected AbstractMvelDrivenOperationImpl(@Nonnull Class<T> resultClass) {
        this.resultClass = resultClass;
    }

    protected static final Set<? extends Parameter> REQUIRED_PARAMETERS =
            Sets.newHashSet(MVEL_EXPRESSIONS);

    protected static final Map<UUID, CachingMapVariableResolverFactory> VARIABLE_RESOLVER_FACTORIES = Maps.newHashMap();

    @Inject
    protected ApplicationContext applicationContext;

    @Inject
    protected MvelExpressionEvaluationService mvelExpressionEvaluationService;

    @Inject
    protected ObjectMapper objectMapper;

    @Nonnull
    @Override
    protected Invocation<T> createInvocation(@Nonnull InvocationContext invocationContext,
                                             @Nonnull Map<String, Object> parameters) {
        return this.initInvocation(new MvelDrivenInvocation<T>(), invocationContext, parameters);
    }

    @Nonnull
    @Override
    protected Invocation<T> initInvocation(@Nonnull Invocation<T> invocation,
                                           @Nonnull InvocationContext invocationContext,
                                           @Nonnull Map<String, Object> parameters) {
        super.initInvocation(invocation, invocationContext, parameters);

        ((MvelDrivenInvocation<T>) invocation).mvelExpression = MapUtils.getString(parameters, MVEL_EXPRESSIONS.getKey());

        return invocation;
    }

    @Nonnull
    @Override
    protected Map<String, Object> manipulateParameters(@Nonnull Map<String, Object> parameters) {
        return parameters;
    }

    @Override
    protected void verifyParameters(@Nonnull Map<String, Object> parameters) {
        super.verifyParameters(parameters);
        verifyParameters(parameters, REQUIRED_PARAMETERS);
    }

    @Nullable
    @Override
    public T eval(@Nonnull Map<String, Object> parameters) {
        var classImports = readClassImports(MapUtils.getObject(parameters, MVEL_CLASS_IMPORTS.getKey(),
                Sets.newHashSet()));
        var packageImports = readPackageImports(MapUtils.getObject(parameters, MVEL_PACKAGE_IMPORTS.getKey(),
                Sets.newHashSet()));
        var parserContext = mvelExpressionEvaluationService.buildParserContext(classImports, packageImports);
        var expression = MapUtils.getString(parameters, MVEL_EXPRESSIONS.getKey());
        var compliedExpression = mvelExpressionEvaluationService.compileExpression(expression,
                parserContext);
        var sessionId = getOrCreateSessionId(parameters);
        var variableResolverFactory = getOrCreateVariableResolverFactory(sessionId,
                thisVariableResolverFactory -> thisVariableResolverFactory.createVariable("parameters", parameters));

        return mvelExpressionEvaluationService.evaluate(compliedExpression, variableResolverFactory, resultClass,
                result -> {
                    var invocationContext = getOrCreateInvocationContext(sessionId);

                    assert ClassUtils.isAssignable(invocationContext.getCurrentInvocation().getClass(),
                            MvelDrivenInvocation.class);

                    @SuppressWarnings("unchecked")
                    var invocation = (MvelDrivenInvocation<T>) invocationContext.getCurrentInvocation();

                    invocation.setResult(result);
                }, exception -> {
                    var invocationContext = getOrCreateInvocationContext(sessionId);

                    assert ClassUtils.isAssignable(invocationContext.getCurrentInvocation().getClass(),
                            MvelDrivenInvocation.class);

                    invocationContext.getCurrentInvocation().setThrowable(exception);
                });
    }

    @Nullable
    @Override
    public T invoke(@Nonnull Map<String, Object> parameters) {
        return withInvocationContext(parameters, this::eval);
    }

    @Nonnull
    protected Map<String, Object> getDefaultVariables(@Nonnull UUID uuid) {
        @SuppressWarnings("unchecked")
        var builtInVariables = (Map<String, Object>) applicationContext.getBean("builtInMvelVariables", Map.class);
        var clone = Maps.newHashMap(builtInVariables);
        clone.put("invocationContext", getOrCreateInvocationContext(uuid));
        clone.put("sessionId", uuid);

        return clone;
    }

    @Nonnull
    protected CachingMapVariableResolverFactory getOrCreateVariableResolverFactory(
            @Nonnull UUID uuid, @Nullable Consumer<CachingMapVariableResolverFactory> callback) {
        Optional<CachingMapVariableResolverFactory> optional;

        if (VARIABLE_RESOLVER_FACTORIES.containsKey(uuid)) {
            optional = Optional.of(MapUtils.getObject(VARIABLE_RESOLVER_FACTORIES, uuid));
        } else {
            var defaultVariables = getDefaultVariables(uuid);

            CachingMapVariableResolverFactory cachingMapVariableResolverFactory
                    = new CachingMapVariableResolverFactory(defaultVariables);

            VARIABLE_RESOLVER_FACTORIES.putIfAbsent(uuid, cachingMapVariableResolverFactory);

            optional = Optional.of(cachingMapVariableResolverFactory);
        }

        if (Objects.nonNull(callback)) {
            optional.ifPresent(callback);
        }

        return optional.get();
    }

    @Nonnull
    protected Set<Class<?>> readClassImports(@Nonnull Object classImports) {
        List<String> fullyQualifiedClassNames;

        if (classImports.getClass().isArray() && classImports.getClass().getComponentType() == String.class) {
            fullyQualifiedClassNames = Arrays.stream(((String[]) classImports)).collect(Collectors.toList());
        } else if (ClassUtils.isAssignable(classImports.getClass(), Collection.class)) {
            fullyQualifiedClassNames = ((Collection<?>) classImports).stream().map(Object::toString)
                    .collect(Collectors.toList());
        } else if (ClassUtils.isAssignable(classImports.getClass(), String.class)) {
            try {
                fullyQualifiedClassNames = objectMapper.readValue(classImports.toString(), new TypeReference<>() {
                });
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            throw new UnsupportedOperationException();
        }

        var classes = Lists.newLinkedList(ClassUtils.convertClassNamesToClasses(
                fullyQualifiedClassNames));

        return Sets.newHashSet(classes);
    }

    @Nonnull
    protected Set<String> readPackageImports(@Nonnull Object packageImports) {
        Set<String> packages;

        if (packageImports.getClass().isArray() && packageImports.getClass().getComponentType() == String.class) {
            packages = Arrays.stream(((String[]) packageImports)).collect(Collectors.toSet());
        } else if (ClassUtils.isAssignable(packageImports.getClass(), Collection.class)) {
            packages = ((Collection<?>) packageImports).stream().map(Object::toString).collect(Collectors.toSet());
        } else if (ClassUtils.isAssignable(packageImports.getClass(), String.class)) {
            try {
                packages = objectMapper.readValue(packageImports.toString(), new TypeReference<>() {
                });
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            throw new UnsupportedOperationException();
        }

        return packages;
    }

    @Getter
    protected static class MvelDrivenInvocation<T> extends Invocation<T> {

        protected String mvelExpression;

    }

}
