package io.openenterprise.daisy.rs

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.Parameter
import io.openenterprise.daisy.rs.model.SuccessResponse
import io.openenterprise.daisy.service.OperationService
import org.apache.commons.lang3.ClassUtils
import org.reflections.Reflections
import org.reflections.util.ConfigurationBuilder
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject

@Component
class DatasetOperationApiImpl(
    @Inject val operationService: OperationService,
    @Inject val objectMapper: ObjectMapper
) : DatasetOperationApi {

    companion object {
        val PARAMETERS_MAP: Map<String, Class<*>> = Reflections(ConfigurationBuilder().forPackage("io.openenterprise"))
            .getSubTypesOf(Parameter::class.java)
            .stream()
            .filter { it.isEnum }
            .map { it.enumConstants }
            .flatMap { Arrays.stream(it) }
            .collect(Collectors.toMap({it.key}, {it.getValueType<Any>()}))
    }

    override fun invokeDatasetOperation(body: MutableMap<String, Any>, beanNameOrClassName: String): SuccessResponse {
        val parameters = convertParametersValues(body)
        val result: Any = operationService.invoke(beanNameOrClassName, parameters)

        return SuccessResponse().result(result).resultClass(result.javaClass.name)
    }

    fun convertParametersValues(parameters: Map<String, Any>): MutableMap<String, Any> {
        val convertedParameters = parameters.entries.stream()
            .filter { PARAMETERS_MAP.containsKey(it.key) }
            .filter { ClassUtils.isAssignable(it.value::class.java, String::class.java) }
            .filter { isValidJson(it.value.toString()) }
            .map { AbstractMap.SimpleEntry<String, Any>(it.key, objectMapper.readerFor(PARAMETERS_MAP[it.key])
                .readValue(it.value.toString())) }
            .collect(Collectors.toMap({it.key}, {it.value}))

        parameters.entries.stream()
            .filter { !convertedParameters.containsKey(it.key) }
            .filter { PARAMETERS_MAP.containsKey(it.key) }
            .filter { canConvert(it.value, PARAMETERS_MAP[it.key] as Class<*>)}
            .forEach {
                convertedParameters[it.key] = objectMapper.convertValue(it.value, PARAMETERS_MAP[it.key] as Class<*>)
            }

        parameters.entries.stream()
            .filter { !convertedParameters.containsKey(it.key) }
            .forEach {
                convertedParameters[it.key] = it.value
            }

        return convertedParameters
    }

    fun isValidJson(string: String): Boolean {
        try {
            objectMapper.readTree(string)
        } catch (e: IOException) {
            return false
        }

        return true
    }

    fun canConvert(obj: Any, targetClass: Class<*>): Boolean {
        try {
            objectMapper.convertValue(obj, targetClass)
        } catch (e: IllegalArgumentException) {
            return false
        }

        return true
    }
}