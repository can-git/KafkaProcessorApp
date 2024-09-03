package com.example.kafkaprocesserapp.model;

import lombok.Data;

@Data
public class AggregatedCombinedDataModel {

    private Integer totalPriceOfFord;
    private Integer totalPriceOfBMW;
    private Integer totalPriceOfToyota;
    private String windowS;
    private String windowE;
}
