package io.openenterprise.daisy.spark.ml.clustering

import com.google.common.collect.Sets
import io.openenterprise.daisy.spark.ml.AbstractTrainOperationImpl
import io.openenterprise.daisy.spark.sql.DatasetUtils
import org.apache.commons.collections4.MapUtils
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.springframework.stereotype.Component
import javax.annotation.Nonnull

@Component("kMeansTrainOperation")
class KMeansTrainOperation : AbstractTrainOperationImpl<KMeansModel?>() {

    @Nonnull
    override fun manipulateParameters(@Nonnull parameters: Map<String, Any>): Map<String, Any> {
        return parameters
    }

    override fun verifyParameters(@Nonnull parameters: Map<String, Any>) {
        super.verifyParameters(parameters)
        super.verifyParameters(parameters, REQUIRED_PARAMETERS)
    }

    @Nonnull
    override fun train(@Nonnull dataset: Dataset<Row>, @Nonnull parameters: Map<String, Any>): KMeansModel {
        @Suppress("UNCHECKED_CAST")
        val featureColumns = MapUtils.getObject(parameters, Parameter.FEATURES_COLUMN.key) as Array<String>
        val transformedDataset = DatasetUtils.createVectorColumn(dataset, "features",
            *featureColumns)

        val k = MapUtils.getInteger(parameters, Parameter.KMEANS_K.key)
        val kMeans = KMeans().setK(k)

        if (parameters.containsKey(Parameter.KMEANS_MAX_ITER.key)) {
            val maxIteration = MapUtils.getInteger(parameters, Parameter.KMEANS_MAX_ITER.key)

            kMeans.setMaxIter(maxIteration)
        }

        return kMeans.fit(transformedDataset)
    }

    companion object {
        protected val REQUIRED_PARAMETERS: Set<io.openenterprise.Parameter> =
            Sets.newHashSet<io.openenterprise.Parameter>(
                Parameter.FEATURES_COLUMN, Parameter.KMEANS_K
            )
    }
}
