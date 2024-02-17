package io.openenterprise.daisy.spark.sql

import io.openenterprise.daisy.Invocation
import io.openenterprise.daisy.Parameter.MVEL_EXPRESSIONS
import org.apache.commons.collections4.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Dataset
import org.springframework.stereotype.Component
import javax.annotation.Nonnull

@Component
class SimpleLoadDatasetOperation : AbstractLoadDatasetOperationImpl() {

    public override fun verifyParameters(@Nonnull parameters: Map<String, Any>) {
        super.verifyParameters(parameters)

        require(
            !(parameters.containsKey(Parameter.DATASET.key) &&
                    parameters.containsKey(Parameter.DATASET_FORMAT.key) &&
                    parameters.containsKey(Parameter.DATASET_TABLE.key) &&
                    parameters.containsKey(Parameter.DATASET_VIEW.key))
        ) {
            Parameter.DATASET.key + " & " +
                    Parameter.DATASET_FORMAT.key + " & " +
                    parameters.containsKey(Parameter.DATASET_TABLE.key) + " & " +
                    Parameter.DATASET_VIEW.key + " are all present"
        }

        require(
            !(!parameters.containsKey(Parameter.DATASET.key) &&
                    !parameters.containsKey(Parameter.DATASET_FORMAT.key) &&
                    !parameters.containsKey(Parameter.DATASET_TABLE.key) &&
                    !parameters.containsKey(Parameter.DATASET_VIEW.key))
        ) {
            Parameter.DATASET.key + "/" + Parameter.DATASET_FORMAT.key +
                    "/" + Parameter.DATASET_TABLE.key + "/" + Parameter.DATASET_VIEW.key + " is required"
        }

        require(
            !(!parameters.containsKey(Parameter.DATASET_FORMAT.key) &&
                    !parameters.containsKey(Parameter.DATASET_TABLE.key) &&
                    !parameters.containsKey(Parameter.DATASET_VIEW.key))
        ) {
            "One of " + Parameter.DATASET_FORMAT.key + ", " +
                    Parameter.DATASET_TABLE.key + ", " + Parameter.DATASET_VIEW.key + " is required"
        }

        if (parameters.containsKey(Parameter.DATASET_FORMAT.key)) {
            val format = MapUtils.getString(parameters, Parameter.DATASET_FORMAT.key)

            if (StringUtils.equals("jdbc", format)) {
                require(parameters.containsKey(Parameter.JDBC_DB_TABLE.key)) { Parameter.JDBC_DB_TABLE.key + " is missing" }

                require(parameters.containsKey(Parameter.JDBC_PASSWORD.key)) { Parameter.JDBC_PASSWORD.key + " is missing" }

                require(parameters.containsKey(Parameter.JDBC_URL.key)) { Parameter.JDBC_URL.key + " is missing" }

                require(parameters.containsKey(Parameter.JDBC_USER.key)) { Parameter.JDBC_USER.key + " is missing" }
            } else {
                require(
                    !(parameters.containsKey(Parameter.DATASET_PATH.key) &&
                            parameters.containsKey(Parameter.DATASET_TABLE.key))
                ) {
                    Parameter.DATASET_PATH.key + " & " +
                            Parameter.DATASET_TABLE.key + " are both present"
                }

                require(
                    !(!parameters.containsKey(Parameter.DATASET_PATH.key) &&
                            !parameters.containsKey(Parameter.DATASET_TABLE.key))
                ) {
                    Parameter.DATASET_PATH.key + " & " +
                            Parameter.DATASET_TABLE.key + " are both missing"
                }
            }
        }
    }

    override fun init(parameters: MutableMap<String, Any>): Invocation<Dataset<*>> {
        val invocation = super.init(parameters)

        addParameters(parameters)

        return invocation
    }

    @Nonnull
    protected fun addParameters(@Nonnull parameters: MutableMap<String, Any>) {
        val sessionId = getSessionId(parameters)
        val invocationContext = getOrCreateInvocationContext(sessionId)
        val currentInvocation = invocationContext.currentInvocation
        val variableName = getDatasetVariable(currentInvocation)
        val mvelExpression = "$variableName = datasetService.loadDataset(parameters);$variableName"

        parameters[MVEL_EXPRESSIONS.key] = mvelExpression
    }
}

