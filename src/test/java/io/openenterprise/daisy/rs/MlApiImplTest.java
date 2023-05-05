package io.openenterprise.daisy.rs;

import io.openenterprise.daisy.examples.ml.ClusterAnalysisOnRecentPurchaseExampleTest;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.*;

@Import(ClusterAnalysisOnRecentPurchaseExampleTest.Configuration.class)
class MlApiImplTest extends AbstractApiTest {

    @Test
    @Order(2)
    void getPrediction() {

    }

    @Test
    @Order(1)
    void trainModel() {
    }

}