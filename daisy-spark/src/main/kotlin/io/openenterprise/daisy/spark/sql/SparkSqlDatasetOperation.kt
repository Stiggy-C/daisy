package io.openenterprise.daisy.spark.sql

import com.google.common.base.CaseFormat
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import io.openenterprise.daisy.AbstractMvelDrivenOperationImpl
import org.apache.commons.collections4.MapUtils
import org.apache.spark.sql.Dataset
import org.springframework.stereotype.Component
import java.util.*

/**
 * Run a Spark SQL statement against given [org.apache.spark.sql.SparkSession]
 */
@Component
class SparkSqlDatasetOperation : AbstractMvelDrivenOperationImpl<Dataset<*>>(Dataset::class.java) {

    override fun manipulateParameters(parameters: Map<String, Any>): Map<String, Any> {
        val sessionId = getSessionId(parameters)
        val invocationContext = getInvocationContext(sessionId)

        assert(Objects.nonNull(invocationContext))
        val currentInvocation = invocationContext!!.currentInvocation
        val variableName = "dataset" + CaseFormat.LOWER_HYPHEN.to(
            CaseFormat.LOWER_UNDERSCORE,
            currentInvocation.id.toString()
        )
        val sql = "parameters[${Parameter.DATASET_SQL.key}]"
        val args = "parameters[${Parameter.DATASET_SQL_PARAMETERS.key}]";

        val mvelExpression = "$variableName = spark.sql($sql, $args);$variableName"

        val manipulated = Maps.newHashMap(parameters)
        manipulated[Parameter.DATASET_SQL_PARAMETERS.key] = MapUtils.getMap(
            parameters, Parameter.DATASET_SQL_PARAMETERS.key,
            emptyMap<String?, Any>()
        )
        manipulated[io.openenterprise.daisy.Parameter.MVEL_EXPRESSIONS.key] = mvelExpression

        return manipulated
    }

    override fun verifyParameters(parameters: Map<String, Any>) {
        super.verifyParameters(parameters)
        super.verifyParameters(parameters, REQUIRED_PARAMETERS)
    }

    companion object {
        private val REQUIRED_PARAMETERS: Set<io.openenterprise.Parameter> =
            Sets.newHashSet<io.openenterprise.Parameter>(Parameter.DATASET_SQL)
    }
}
