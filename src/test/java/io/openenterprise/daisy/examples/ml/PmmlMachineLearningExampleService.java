package io.openenterprise.daisy.examples.ml;

import io.openenterprise.daisy.spark.ml.AbstractPmmlMachineLearningService;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component("pmmlBasedMachineLearningExample")
@Profile("ml_example")
public class PmmlMachineLearningExampleService extends AbstractPmmlMachineLearningService {
}
