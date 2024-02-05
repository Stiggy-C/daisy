package io.openenterprise.daisy.test;

import io.openenterprise.daisy.example.data.SkuCategory;
import io.openenterprise.daisy.example.data.Transaction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.ResourceUtils;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TransactionsCsvGenerator {

    @Value("${daisy.example.transactions-csv.size:10000}")
    private int numberOfTransactions;

    public void generate(@Nonnull List<String> memberIds, @Nonnull String fileUri) throws IOException {
        File file = ResourceUtils.getFile(fileUri);

        // Step 1: Generate transactions:
        var transactions = new ArrayList<Transaction>(numberOfTransactions);

        for (int i = 0; i < numberOfTransactions; i++) {
            var memberId = memberIds.get(RandomUtils.nextInt(0, memberIds.size()));
            transactions.add(generateTransaction(memberId));
        }

        // Step 2: Print the csv:
        var headers = new String[] {"id", "memberId", "skuId", "skuCategory", "skuPrice", "createdDateTime"};

        var csvFormat = CSVFormat.DEFAULT.builder().setHeader(headers).build();
        var fileWriter = new FileWriter(file);

        try (var csvPrinter = new CSVPrinter(fileWriter, csvFormat)) {
            for (var transaction : transactions) {
                csvPrinter.printRecord(transaction.getId(), transaction.getMemberId(), transaction.getSkuId(),
                        transaction.getSkuCategory(), transaction.getSkuPrice(), transaction.getCreatedDateTime());
            }
        }
    }

    protected Transaction generateTransaction(@Nonnull String memberId) {
        var skuCategory = SkuCategory.values()[RandomUtils.nextInt(0, SkuCategory.values().length)];

        var transaction = new Transaction();
        transaction.setId(UUID.randomUUID().toString());
        transaction.setMemberId(memberId);
        transaction.setSkuId(UUID.randomUUID().toString());
        transaction.setSkuCategory(skuCategory);
        transaction.setSkuPrice(BigDecimal.valueOf(RandomUtils.nextDouble(10, 10000)).setScale(2, RoundingMode.HALF_UP));
        transaction.setCreatedDateTime(OffsetDateTime.now());

        return transaction;
    }
}
