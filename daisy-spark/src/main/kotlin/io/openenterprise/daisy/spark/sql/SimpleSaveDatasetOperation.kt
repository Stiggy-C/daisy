package io.openenterprise.daisy.spark.sql

import io.openenterprise.daisy.Invocation
import io.openenterprise.daisy.mvel2.integration.impl.CachingMapVariableResolverFactory
import io.openenterprise.daisy.spark.sql.Parameter.DATASET
import io.openenterprise.daisy.spark.sql.Parameter.DATASET_VARIABLE
import io.openenterprise.daisy.spark.sql.service.DatasetService
import org.apache.commons.lang3.ClassUtils
import org.apache.commons.lang3.ObjectUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.springframework.stereotype.Component
import java.util.*
import java.util.function.Consumer
import javax.annotation.Nonnull
import javax.inject.Inject

@Component
class SimpleSaveDatasetOperation : AbstractSaveDatasetOperationImpl() {

    @Inject
    private var datasetService: DatasetService? = null

    @Nonnull
    override fun init(@Nonnull parameters: MutableMap<String, Any>): Invocation<Void> {
        val invocation = super.init(parameters)

        addParameters(parameters)

        return invocation
    }

    @Nonnull
    override fun getOrCreateVariableResolverFactory(
        @Nonnull uuid: UUID, callback: Consumer<CachingMapVariableResolverFactory>?
    ): CachingMapVariableResolverFactory {
        val cachingMapVariableResolverFactory =
            super.getOrCreateVariableResolverFactory(uuid, callback)

        cachingMapVariableResolverFactory.createVariable("datasetService", datasetService)

        return cachingMapVariableResolverFactory
    }

    override fun getDataset(parameters: Map<String, Any?>): Dataset<Row>? {
        val dataset =
            if (parameters.containsKey(DATASET.key) && Objects.nonNull(parameters[DATASET.key])) {
                @Suppress("UNCHECKED_CAST")
                parameters[DATASET.key] as Dataset<Row>
            } else if (parameters.containsKey(DATASET_VARIABLE.key) &&
                ObjectUtils.isNotEmpty(parameters[DATASET_VARIABLE.key]) &&
                ClassUtils.isAssignable(parameters[DATASET_VARIABLE.key]!!.javaClass, String::class.java)
            ) {
                val sessionId = getSessionId(parameters)
                val cachingMapVariableResolverFactory =
                    getOrCreateVariableResolverFactory(sessionId, null)
                val variableResolver = cachingMapVariableResolverFactory
                    .getVariableResolver(parameters[DATASET_VARIABLE.key].toString())

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
        val mvelExpression = "datasetService.saveDataset(parameters[\"${DATASET.key}\"], parameters);"

        parameters[io.openenterprise.daisy.Parameter.MVEL_EXPRESSIONS.key] = mvelExpression
    }

}
