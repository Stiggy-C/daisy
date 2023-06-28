package io.openenterprise.daisy.examples.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.openenterprise.daisy.spark.ml.AbstractMachineLearningService;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Component
@Profile("ml_example")
public class HongKongMark6LotteryResultFrequentPatternMining extends AbstractMachineLearningService<FPGrowthModel> {

    protected HongKongMark6LotteryResultFrequentPatternMining() {
        super(FPGrowthModel.class);
    }

    @NotNull
    @Override
    protected FPGrowthModel buildModel(@NotNull Dataset<Row> dataset, @NotNull Map<String, ?> parameters) {
        var fpGrowth = new FPGrowth().
                setMinSupport(1.0/2134); // hk_mark_6_results_20080103-20230520.csv has 2134 rows, want min 1 appearance

        return fpGrowth.fit(dataset);
    }

    @NotNull
    @Override
    public Dataset<Row> predict(@NotNull FPGrowthModel model, @NotNull String jsonString, @NotNull Map<String, ?> parameters)
            throws JsonProcessingException {
        /*
            From io.openenterprise.daisy.examples.ml.HongKongMark6LotteryResultClusterAnalysis, know that the min count
            of the lowest tier of number (i.e. the least frequent numbers) is 274. Since we want to know the top 50
            most frequent pattern (i.e. not just 1 number) of the combination, we can ignore freq >= 274.
         */
        return model.freqItemsets().where(col("freq").lt(274)).orderBy(col("freq").desc())
                .limit(50);
    }

    @NotNull
    @Override
    public Dataset<Row> buildDataset(@NotNull Map<String, ?> parameters) {
        // Get csvS3Url from given list of parameters:
        String csvS3Url = parameters.get("csvS3Uri").toString();

        // Build source dataset:
        return sparkSession.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvS3Url)
                .select(array(col("Winning Number 1"), col("2"), col("3"), col("4"),
                        col("5"), col("6"), col("Extra Number ")).as("items"))
                .as("winningNumbers");
    }
}
