package io.openenterprise.daisy.test.spark;

import io.openenterprise.daisy.test.MvelDrivenOperationTest;
import org.springframework.context.annotation.Import;

@Import({MvelDrivenOperationTest.Configuration.class, Configuration.class})
public class AbstractTest extends io.openenterprise.daisy.test.AbstractTest {
}
