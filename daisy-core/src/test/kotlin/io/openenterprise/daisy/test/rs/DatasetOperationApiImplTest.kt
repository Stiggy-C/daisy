package io.openenterprise.daisy.test.rs

import com.google.common.collect.Maps
import io.openenterprise.daisy.MvelDrivenOperation
import io.openenterprise.daisy.MvelDrivenOperationImpl
import io.openenterprise.daisy.Parameter
import io.openenterprise.daisy.rs.DatasetOperationApiImpl
import io.openenterprise.daisy.springframework.boot.autoconfigure.ApplicationConfiguration
import io.openenterprise.daisy.springframework.boot.autoconfigure.cache.TCacheConfiguration
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Import
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringExtension
import kotlin.test.assertEquals

@ExtendWith(SpringExtension::class)
@Import(ApplicationConfiguration::class)
@TestPropertySource(properties = ["daisy.s3.bucket=daisy", "spring.profiles.active=local_spark,pipeline_example,tCache"])
class DatasetOperationApiImplTest: AbstractApiTest() {

    companion object {

        private val TEST_PARAMETERS = Maps.newHashMap<String, Any>(java.util.Map.of(
            Parameter.MVEL_CLASS_IMPORTS.key, "[\"org.testcontainers.shaded.org.apache.commons.lang3.StringUtils\"]",
            Parameter.MVEL_EXPRESSIONS.key, "StringUtils.isEmpty(\"\")",
            Parameter.MVEL_PACKAGE_IMPORTS.key, arrayListOf("io.openeterprise")
        ))

    }

    @Autowired
    private var datasetOperationApiImpl: DatasetOperationApiImpl? = null

    @Test
    fun testInvokeDatasetOperation() {
        val result = datasetOperationApiImpl!!.invokeDatasetOperation(
            TEST_PARAMETERS,
            MvelDrivenOperationImpl::class.java.name)

        assertNotNull(result)
        assertNotNull(result.result)
        assertNotNull(result.resultClass)
        assertTrue(result.result as Boolean)
        assertEquals(result.resultClass::class.java.name, String::class.java.name)
    }

    @Test
    fun testConvertParametersValues() {
        val converted = datasetOperationApiImpl!!.convertParametersValues(TEST_PARAMETERS)

        assertNotNull(converted)
        assertTrue(converted.containsKey(Parameter.MVEL_CLASS_IMPORTS.key))
        assertTrue(converted[Parameter.MVEL_CLASS_IMPORTS.key]!!.javaClass.isArray)
    }
}