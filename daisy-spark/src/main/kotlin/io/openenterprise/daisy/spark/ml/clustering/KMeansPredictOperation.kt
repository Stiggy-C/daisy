package io.openenterprise.daisy.spark.ml.clustering

import io.openenterprise.daisy.spark.ml.AbstractPredictOperationImpl
import io.openenterprise.daisy.spark.sql.DatasetUtils
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.springframework.stereotype.Component
import java.util.*
import javax.annotation.Nonnull


@Component("kMeansPredictOperation")
class KMeansPredictOperation : AbstractPredictOperationImpl<KMeansModel>(KMeansModel::class.java) {

    @Nonnull
    override fun getDataset(@Nonnull parameters: Map<String, Any>): Dataset<Row> {
        val sessionId = getSessionId(parameters)
        val invocationContext = this.getOrCreateInvocationContext(sessionId)

        val dataset = DatasetUtils.getDataset(invocationContext)

        assert(Objects.nonNull(dataset))
        return dataset!!
    }

    @Nonnull
    override fun manipulateParameters(@Nonnull parameters: Map<String, Any>): Map<String, Any> {
        return parameters
    }
}
