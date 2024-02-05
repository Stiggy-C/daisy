package io.openenterprise.daisy.spark.ml.fpm

import com.google.common.collect.Sets
import io.openenterprise.daisy.spark.ml.AbstractTrainOperationImpl
import org.apache.spark.ml.fpm.FPGrowthModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.springframework.stereotype.Component

@Component
class FPGrowthTrainOperation: AbstractTrainOperationImpl<FPGrowthModel>() {
    override fun manipulateParameters(parameters: MutableMap<String, Any>): MutableMap<String, Any> {
        return parameters
    }

    override fun verifyParameters(parameters: MutableMap<String, Any>) {
        super.verifyParameters(parameters)

        verifyParameters(parameters, REQUIRED_PARAMETERS)
    }

    override fun train(dataset: Dataset<Row>, parameters: MutableMap<String, Any>): FPGrowthModel {
        TODO("Not yet implemented")
    }

    companion object {

        protected val REQUIRED_PARAMETERS: Set<io.openenterprise.Parameter> =
            Sets.newHashSet<io.openenterprise.Parameter>(Parameter.MIN_SUPPORT);
    }
}