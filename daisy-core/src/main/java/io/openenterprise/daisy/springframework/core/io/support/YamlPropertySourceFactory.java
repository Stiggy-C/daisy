package io.openenterprise.daisy.springframework.core.io.support;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;

/**
 * Referenced from <a href="https://www.baeldung.com/spring-yaml-propertysource">here</a>.
 */
public class YamlPropertySourceFactory implements PropertySourceFactory {

    @Override
    public PropertySource<?> createPropertySource(@Nonnull String name, @Nonnull EncodedResource encodedResource) throws IOException {
        var resource = encodedResource.getResource();

        var yamlPropertiesFactoryBean = new YamlPropertiesFactoryBean();
        yamlPropertiesFactoryBean.setResources(resource);

        var properties = yamlPropertiesFactoryBean.getObject();

        return new PropertiesPropertySource(Objects.requireNonNull(resource.getFilename()), Objects.requireNonNull(properties));
    }
}
