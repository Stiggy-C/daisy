package io.openenterprise.daisy.rs;

import io.openenterprise.daisy.examples.Configuration;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

@ExtendWith(SpringExtension.class)
@Import(Configuration.class)
@WebMvcTest
public abstract class AbstractApiTest {

    protected static final String CONTEXT_PATH = "/services/";

    @Autowired
    protected MockMvc mockMvc;

}
