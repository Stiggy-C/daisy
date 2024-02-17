package io.openenterprise.daisy.spark.sql.streaming

import io.openenterprise.daisy.Invocation
import io.openenterprise.daisy.mvel2.integration.impl.CachingMapVariableResolverFactory
import io.openenterprise.daisy.spark.sql.DatasetUtils
import io.openenterprise.daisy.spark.sql.Parameter
import io.openenterprise.daisy.spark.sql.streaming.service.StreamingDatasetService
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.lang3.ObjectUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamingQuery
import org.springframework.stereotype.Component
import java.util.*
import java.util.function.Consumer
import javax.annotation.Nonnull
import javax.inject.Inject

@Component
class SimpleSaveStreamingDatasetOperation(@Inject var streamingDatasetService: StreamingDatasetService) :
    AbstractSaveStreamingDatasetOperationImpl() {

    @Nonnull
    override fun getOrCreateVariableResolverFactory(
        @Nonnull uuid: UUID, callback: Consumer<CachingMapVariableResolverFactory>?
    ): CachingMapVariableResolverFactory {
        val cachingMapVariableResolverFactory =
            super.getOrCreateVariableResolverFactory(uuid, callback)

        cachingMapVariableResolverFactory.createVariable("streamingDatasetService", streamingDatasetService)

        return cachingMapVariableResolverFactory
    }

    override fun init(parameters: MutableMap<String, Any>): Invocation<StreamingQuery> {
        val invocation = super.init(parameters)

        addParameters(parameters)

        return invocation
    }

    override fun getDataset(parameters: MutableMap<String, Any>): Dataset<Row>? {
        val dataset =
            if (parameters.containsKey(Parameter.DATASET.key) && Objects.nonNull(parameters[Parameter.DATASET.key])) {
                @Suppress("UNCHECKED_CAST")
                parameters[Parameter.DATASET.key] as Dataset<Row>
            } else if (parameters.containsKey(Parameter.DATASET_VARIABLE.key) &&
                ObjectUtils.isNotEmpty(parameters[Parameter.DATASET_VARIABLE.key]) &&
                ClassUtils.isAssignable(parameters[Parameter.DATASET_VARIABLE.key]!!.javaClass, String::class.java)
            ) {
                val sessionId = getSessionId(parameters)
                val cachingMapVariableResolverFactory =
                    getOrCreateVariableResolverFactory(sessionId, null)
                val variableResolver = cachingMapVariableResolverFactory
                    .getVariableResolver(parameters[Parameter.DATASET_VARIABLE.key].toString())

                assert(Objects.nonNull(variableResolver))

                @Suppress("UNCHECKED_CAST")
                variableResolver.value as Dataset<Row>
            } else {
                val sessionId = getSessionId(parameters)
                val invocationContext = this.getOrCreateInvocationContext(sessionId)

                DatasetUtils.getDataset(invocationContext)
            }

        return dataset
    }

    private fun addParameters(parameters: MutableMap<String, Any>) {
        val sessionId = getSessionId(parameters)
        val invocationContext = getInvocationContext(sessionId)

        assert(Objects.nonNull(invocationContext))

        val streamingQueryVariable = getStreamingQueryVariable(invocationContext!!.currentInvocation)
        val mvelExpression =
            "$streamingQueryVariable = streamingDatasetService.saveDataset(parameters[\"${Parameter.DATASET.key}\"], parameters); $streamingQueryVariable;"

        parameters[io.openenterprise.daisy.Parameter.MVEL_EXPRESSIONS.key] = mvelExpression
    }
}