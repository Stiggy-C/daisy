package io.openenterprise.daisy.spark.sql

import com.google.common.base.CaseFormat
import io.openenterprise.daisy.Invocation
import org.apache.commons.compress.utils.Sets
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.tuple.Pair
import org.apache.spark.sql.Dataset
import org.springframework.stereotype.Component
import javax.annotation.Nonnull

@Component
class SimpleJoinDatasetOperation : AbstractJoinDatasetOperationImpl() {

    companion object {

        val REQUIRED_PARAMETERS: Set<io.openenterprise.Parameter> = Sets.newHashSet(
            Parameter.DATASET_JOIN_COLUMNS,
            Parameter.DATASET_PAIR
        )
    }

    override fun init(parameters: MutableMap<String, Any?>): Invocation<Dataset<*>> {
        val invocation = super.init(parameters)

        addParameters(parameters)

        return invocation
    }

    override fun verifyParameters(parameters: MutableMap<String, Any?>) {
        super.verifyParameters(parameters)

        verifyParameters(parameters, REQUIRED_PARAMETERS)
    }

    @Nonnull
    protected fun addParameters(@Nonnull parameters: MutableMap<String, Any?>) {
        assert(parameters.containsKey(Parameter.DATASET_PAIR.key))
        assert(parameters.containsKey(Parameter.DATASET_JOIN_COLUMNS.key))

        @Suppress("UNCHECKED_CAST")
        val datasetPair = parameters[Parameter.DATASET_PAIR.key] as Pair<String, String>

        @Suppress("UNCHECKED_CAST")
        val joinColumns = parameters[Parameter.DATASET_JOIN_COLUMNS.key] as Pair<String, String>

        assert(StringUtils.isNotEmpty(datasetPair.left))
        assert(StringUtils.isNotEmpty(datasetPair.right))

        val left = datasetPair.left
        val right = datasetPair.right

        assert(StringUtils.isNotEmpty(joinColumns.left))
        assert(StringUtils.isNotEmpty(joinColumns.right))

        val joinColumnLeft = joinColumns.left
        val joinColumnRight = joinColumns.right

        val joinType =
            if (parameters.containsKey(Parameter.DATASET_JOIN_TYPE.key))
                parameters[Parameter.DATASET_JOIN_TYPE.key] as String
            else
                "left"

        val sessionId = getSessionId(parameters)
        val invocationContext = getOrCreateInvocationContext(sessionId)
        val currentInvocation = invocationContext.currentInvocation
        val variableName = "dataset" + CaseFormat.LOWER_HYPHEN.to(
            CaseFormat.LOWER_UNDERSCORE,
            currentInvocation.id.toString()
        )

        val mvelExpression = "$variableName = $left.join($right, " +
                "sql.col(\"$joinColumnLeft\").equalTo(sql.col(\"$joinColumnRight\")), \"$joinType\");$variableName"

        parameters[io.openenterprise.daisy.Parameter.MVEL_EXPRESSIONS.key] = mvelExpression
    }
}