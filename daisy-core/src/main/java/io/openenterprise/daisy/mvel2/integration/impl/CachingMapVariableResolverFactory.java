package io.openenterprise.daisy.mvel2.integration.impl;

import com.google.common.collect.Maps;
import org.mvel2.integration.VariableResolver;

import javax.annotation.Nonnull;
import java.util.Map;

public class CachingMapVariableResolverFactory extends org.mvel2.integration.impl.CachingMapVariableResolverFactory {

    protected Map<String, Object> variables;

    public CachingMapVariableResolverFactory() {
        this(Maps.newHashMap());
    }

    public CachingMapVariableResolverFactory(@Nonnull Map<String, Object> variables) {
        super(variables);

        this.variables = variables;
    }

    @Override
    public VariableResolver createVariable(String name, Object value) {
        var variableResolver = super.createVariable(name, value);
        variables.put(name, value);

        return variableResolver;
    }

    @Override
    public VariableResolver createVariable(String name, Object value, Class<?> type) {
        var variableResolver = super.createVariable(name, value, type);

        variables.put(name, value);

        return variableResolver;
    }
}
