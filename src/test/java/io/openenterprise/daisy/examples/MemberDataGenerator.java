package io.openenterprise.daisy.examples;

import io.openenterprise.daisy.examples.data.Gender;
import io.openenterprise.daisy.examples.data.MemberTier;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MemberDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(MemberDataGenerator.class);

    @Autowired
    @Qualifier("mysqlDataSource")
    protected DataSource dataSource;

    public void generate(@Nonnull List<String> memberIds) {
        var jdbcTemplate = new JdbcTemplate(dataSource);

        // Step 1: Create the table if necessary:
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS member (\n" +
                "\tid varchar(36) PRIMARY KEY,\n" +
                "\tfirst_name varchar(32),\n" +
                "\tlast_name varchar(32),\n" +
                "\temail varchar(256) NOT NULL,\n" +
                "\tage tinyint NOT NULL,\n" +
                "\tgender varchar(6) NOT NULL,\n" +
                "\ttier varchar(10) NOT NULL,\n" +
                "\tcreated_date_time datetime NOT NULL,\n" +
                "\tlast_updated_date_time datetime\n" +
                ")");

        // Step 2: Generate members' data:
        var batchArgs = memberIds.stream().map(memberId -> {
            var firstName = RandomStringUtils.randomAlphabetic(8, 16);
            var lastName = RandomStringUtils.randomAlphabetic(8, 16);
            var email = RandomStringUtils.randomAlphabetic(8, 16) + "@" + RandomStringUtils.randomAlphabetic(4, 8) + ".com";
            var age = RandomUtils.nextInt(0, 99);
            var gender = Gender.values()[RandomUtils.nextInt(0, Gender.values().length)].name();
            var tier = MemberTier.values()[RandomUtils.nextInt(0, MemberTier.values().length)].name();
            var createdDateTime = OffsetDateTime.now().minus(RandomUtils.nextInt(1, 48), ChronoUnit.MONTHS);
            var lastUpdatedDateTime = createdDateTime.plus(RandomUtils.nextInt(1, 42), ChronoUnit.MONTHS);

            return new Object[] {memberId, firstName, lastName, email, age, gender, tier, createdDateTime, lastUpdatedDateTime};
        }).collect(Collectors.toList());

        // Step 3: Insert into database:
        var insertSql = "INSERT INTO member VALUES (?, ?, ? ,? , ?, ?, ?, ?, ?)";

        var batchUpdateResult = jdbcTemplate.batchUpdate(insertSql, batchArgs);
        var totalRowsInserted = Arrays.stream(batchUpdateResult).sum();

        LOG.debug("Total # of rows inserted: {}", totalRowsInserted);
    }
}
