package io.openenterprise.daisy.example.data;

import lombok.Data;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Data
public class Transaction {

    protected String id;

    protected String memberId;

    protected String skuId;

    protected SkuCategory skuCategory;

    protected BigDecimal skuPrice;

    protected OffsetDateTime createdDateTime;
}
