package io.openenterprise.daisy.mvel2.integration.impl;

import org.apache.hadoop.shaded.com.google.common.collect.Maps;

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

    @Nonnull
    public Map<String, Object> getVariables() {
        return variables;
    }
}
