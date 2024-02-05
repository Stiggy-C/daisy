package io.openenterprise.daisy.test;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;

@Import(Configuration.class)
public abstract class AbstractTest {

    public static final String TEST_S3_BUCKET = "unit-test";

    @Autowired
    protected AmazonS3 amazonS3;

    @Autowired
    protected MemberDataGenerator memberDataGenerator;

    @Value("${daisy.example.transactions-csv.size:100}")
    protected int numberOfTransactions;

    @Autowired
    protected TransactionsCsvGenerator transactionsCsvGenerator;

    protected void generateExampleTransactions() throws IOException {
        var numberOfMembers = Double.valueOf(Math.floor((double) numberOfTransactions/5));
        var memberIds = new ArrayList<String>(numberOfMembers.intValue());

        for (int i = 0; i < numberOfMembers; i++) {
            memberIds.add(UUID.randomUUID().toString());
        }

        var exampleTransactionCsvUri = "file://" + System.getProperty("user.dir") + "/example/transactions.csv";
        var csvFile = ResourceUtils.getFile(exampleTransactionCsvUri);

        // Truncate example transaction CSV:
        FileUtils.write(csvFile, "", StandardCharsets.UTF_8);

        // Generate a new example transaction CSV:
        transactionsCsvGenerator.generate(memberIds, exampleTransactionCsvUri);

        // Put the example transaction CSV on to Localstack's S3 bucket:
        amazonS3.createBucket(TEST_S3_BUCKET);
        amazonS3.putObject(TEST_S3_BUCKET, "csv_files/transactions.csv", csvFile);

        // Generate the member table:
        memberDataGenerator.generate(memberIds);
    }

    @PostConstruct
    protected void postConstruct() throws IOException {
        generateExampleTransactions();
    }
}
