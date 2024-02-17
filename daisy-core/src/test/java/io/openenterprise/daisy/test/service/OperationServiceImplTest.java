package io.openenterprise.daisy.test.service;

import com.google.common.collect.Maps;
import io.openenterprise.daisy.MvelDrivenOperation;
import io.openenterprise.daisy.MvelDrivenOperationImpl;
import io.openenterprise.daisy.service.OperationService;
import io.openenterprise.daisy.springframework.boot.autoconfigure.ApplicationConfiguration;
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration;
import io.openenterprise.daisy.Parameter;
import io.openenterprise.daisy.test.AbstractTest;
import io.openenterprise.daisy.springframework.context.support.ApplicationContextUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;


@ExtendWith(SpringExtension.class)
@Import({ApplicationConfiguration.class, TCacheConfiguration.class, OperationServiceImplTest.Configuration.class})
@TestPropertySource(properties = {"daisy.aws.s3.bucket=daisy", "spring.profiles.active=local_spark,pipeline_example,tCache"})
class OperationServiceImplTest extends AbstractTest {

    protected static final Map<String, Object> PARAMETERS = Maps.newHashMap(
            Map.of(Parameter.MVEL_EXPRESSIONS.getKey(), "true"));

    @Autowired
    protected OperationService operationService;

    @Test
    void testInvokeByBeanName() {
        var component = MvelDrivenOperationImpl.class.getAnnotation(Component.class);
        var beanName = StringUtils.isEmpty(component.value())?
                StringUtils.uncapitalize(MvelDrivenOperationImpl.class.getSimpleName()) : component.value();

        Boolean bool = Assertions.assertDoesNotThrow(() -> operationService.invoke(beanName, PARAMETERS));

        Assertions.assertTrue(bool);
    }

    @Test
    void testInvokeByClassName() {
        Boolean bool = Assertions.assertDoesNotThrow(() -> operationService.invoke(MvelDrivenOperation.class.getName(),
                PARAMETERS));

        Assertions.assertTrue(bool);
    }

    @Test
    void testInvokeByClass() {
        var mvelDrivenDatasetOperation = ApplicationContextUtils.getApplicationContext().getBean(
                MvelDrivenOperationImpl.class);

        Object result = Assertions.assertDoesNotThrow(() -> operationService.invoke(mvelDrivenDatasetOperation, PARAMETERS));

        Assertions.assertEquals(result, true);
    }

    @TestConfiguration
    public static class Configuration {

        @Bean
        protected MvelDrivenOperation<Object> mvelDrivenOperation() {
            return new MvelDrivenOperationImpl();
        }

    }
}