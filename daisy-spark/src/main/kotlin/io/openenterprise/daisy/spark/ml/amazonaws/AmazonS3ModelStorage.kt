package io.openenterprise.daisy.spark.ml.amazonaws

import com.amazonaws.services.s3.AmazonS3
import io.awspring.cloud.core.io.s3.SimpleStorageResource
import io.openenterprise.daisy.spark.ml.ModelStorage
import lombok.SneakyThrows
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.MLWritable
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.core.task.SyncTaskExecutor
import java.net.URI
import javax.annotation.Nonnull
import javax.annotation.PostConstruct
import javax.inject.Inject
import javax.inject.Named

@Named
@ConditionalOnBean(AmazonS3::class)
class AmazonS3ModelStorage : ModelStorage {

    @Inject
    private var amazonS3: AmazonS3? = null

    @Value("\${daisy.spark.model.directory:/ml/models}")
    private val directory: String? = null

    @Value("\${daisy.s3.bucket}")
    private var s3Bucket: String? = null

    /**
     * Get the S3 URI of the model which has the given modelId.
     *
     * @param modelId
     * @return
     */
    @Nonnull
    override fun getURIOfModel(@Nonnull modelId: String): URI {
        val s3path = (if (StringUtils.startsWith(directory, "/")) StringUtils.replaceOnce(
            directory,
            "/",
            ""
        ) else directory) + "/" + modelId
        val s3Resource = SimpleStorageResource(amazonS3, s3Bucket, s3path, SyncTaskExecutor())

        return s3Resource.s3Uri
    }

    @SneakyThrows
    @Suppress("UNCHECKED_CAST")
    override fun <M> load(@Nonnull modelClass: Class<M>, uid: String): M where M : Transformer?, M : MLWritable? {
        val s3Uri = getURIOfModel(uid)

        return MethodUtils.invokeStaticMethod(
            modelClass, "load", StringUtils.replace(
                s3Uri.toString(), "s3", "s3a"
            )
        ) as M
    }


    @SneakyThrows
    override fun <M> save(model: M): URI where M : Transformer?, M : MLWritable {
        val s3Uri = getURIOfModel(model!!.uid())

        model.write().overwrite().save(StringUtils.replace(s3Uri.toString(), "s3", "s3a"))

        return s3Uri
    }

    @PostConstruct
    private fun postConstruct() {
        if (!amazonS3!!.doesBucketExistV2(s3Bucket)) {
            amazonS3!!.createBucket(s3Bucket)
        }
    }
}
