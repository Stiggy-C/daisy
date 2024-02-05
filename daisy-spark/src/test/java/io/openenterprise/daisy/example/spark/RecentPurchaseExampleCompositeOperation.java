package io.openenterprise.daisy.example.spark;

import com.amazonaws.services.s3.AmazonS3;
import io.openenterprise.daisy.test.MemberDataGenerator;
import io.openenterprise.daisy.Parameter;
import io.openenterprise.daisy.CompositeOperation;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static io.openenterprise.daisy.Parameter.MVEL_EXPRESSIONS;
import static io.openenterprise.daisy.spark.sql.Parameter.*;

@Component("recentPurchaseExample")
@ConditionalOnBean(AmazonS3.class)
@Profile("pipeline_example")
public class RecentPurchaseExampleCompositeOperation extends CompositeOperation {

    @Inject
    private AmazonS3 amazonS3;

    @Value("${recentPurchaseExamplePipeline.mySqlJdbcPassword}")
    private String mySqlJdbcPassword;

    @Value("${recentPurchaseExamplePipeline.mySqlJdbcUrl}")
    private String mySqlJdbcUrl;

    @Value("${recentPurchaseExamplePipeline.mySqlJdbcUser}")
    private String mySqlJdbcUser;

    @Value("${recentPurchaseExamplePipeline.postgresJdbcPassword}")
    private String postgresJdbcPassword;

    @Value("${recentPurchaseExamplePipeline.postgresJdbcUrl}")
    private String postgresJdbcUrl;

    @Value("${recentPurchaseExamplePipeline.postgresJdbcUser}")
    private String postgresJdbcUser;

    @Override
    public Void invoke(@Nonnull Map<String, Object> parameters) {
        var csvS3Uri = MapUtils.getString(parameters, DATASET_PATH.getKey());

        var datasetOperationsBeanNamesOrClasses = List.of("simpleLoadDatasetOperation",
                "simpleLoadDatasetOperation", /*"mvelDrivenOperation"*/"simpleJoinDatasetOperation",
                "simpleSaveDatasetOperation");

        var parameters0 = Maps.newHashMap(Map.<String, Object>of(CSV_HEADER.getKey(), true,
                CSV_INFER_SCHEMA.getKey(), true, DATASET_FORMAT.getKey(),
                "csv", DATASET_PATH.getKey(), csvS3Uri));
        var parameters1 = Maps.newHashMap(Map.<String, Object>of(DATASET_FORMAT.getKey(), "jdbc",
                JDBC_DB_TABLE.getKey(), MemberDataGenerator.DB_TABLE,
                JDBC_DRIVER_CLASS.getKey(), com.mysql.cj.jdbc.Driver.class.getName(),
                JDBC_PASSWORD.getKey(), mySqlJdbcPassword,
                JDBC_USER.getKey(), mySqlJdbcUser,
                JDBC_URL.getKey(), mySqlJdbcUrl));
        /*var parameters2 = Maps.newHashMap(Map.<String, Object>of(MVEL_EXPRESSIONS.getKey(),
                "csvDataset = invocationContext.pastInvocations[1].result.select(\"memberId\", \"skuId\", \"skuCategory\", \"skuPrice\", \"createdDateTime\");" +
                        "jdbcDataset = invocationContext.pastInvocations[2].result.select(\"id\", \"age\", \"gender\", \"tier\");" +
                        "jointDataset = jdbcDataset.join(csvDataset, jdbcDataset.col(\"id\").equalTo(csvDataset.col(\"memberId\")));" +
                        "jointDataset"));*/
        var parameters2 = Maps.newHashMap(Map.<String, Object>of(
                DATASET_PAIR.getKey(), Pair.of("invocationContext.pastInvocations[2].result",
                        "invocationContext.pastInvocations[1].result"),
                DATASET_JOIN_COLUMNS.getKey(), Pair.of("id", "memberId")
                ));
        var parameters3 = Maps.newHashMap(Map.<String, Object>of(DATASET_FORMAT.getKey(), "postgresql",
                JDBC_DB_TABLE.getKey(), "members_purchases",
                JDBC_DRIVER_CLASS.getKey(), org.postgresql.Driver.class.getName(),
                JDBC_PASSWORD.getKey(), postgresJdbcPassword,
                JDBC_USER.getKey(), postgresJdbcUser,
                JDBC_URL.getKey(), postgresJdbcUrl));

        var datasetOperationsParameters = List.of(parameters0, parameters1, parameters2, parameters3);

        parameters = Maps.newHashMap(parameters);

        parameters.put(Parameter.DATASET_OPERATIONS.getKey(), datasetOperationsBeanNamesOrClasses);
        parameters.put(Parameter.PARAMETERS.getKey(), datasetOperationsParameters);

        return super.invoke(parameters);
    }
}
