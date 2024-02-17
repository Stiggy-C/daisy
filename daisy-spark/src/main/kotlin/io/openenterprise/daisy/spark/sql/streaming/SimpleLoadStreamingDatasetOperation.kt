package io.openenterprise.daisy.spark.sql.streaming

import io.openenterprise.daisy.Parameter
import io.openenterprise.daisy.spark.sql.SimpleLoadDatasetOperation
import org.springframework.stereotype.Component
import javax.annotation.Nonnull

@Component
class SimpleLoadStreamingDatasetOperation:  SimpleLoadDatasetOperation() {

    @Nonnull
    override fun addParameters(@Nonnull parameters: MutableMap<String, Any>) {
        val sessionId = getSessionId(parameters)
        val invocationContext = getOrCreateInvocationContext(sessionId)
        val currentInvocation = invocationContext.currentInvocation
        val variableName = getDatasetVariable(currentInvocation)
        val mvelExpression = "$variableName = streamingDatasetService.loadDataset(parameters);$variableName"

        parameters[Parameter.MVEL_EXPRESSIONS.key] = mvelExpression
    }

}