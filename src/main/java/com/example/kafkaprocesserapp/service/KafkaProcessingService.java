package com.example.kafkaprocesserapp.service;

import com.example.kafkaprocesserapp.model.AggregatedCombinedDataModel;
import com.example.kafkaprocesserapp.model.CarModel;
import com.example.kafkaprocesserapp.model.CombinedDataModel;
import com.example.kafkaprocesserapp.utils.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class KafkaProcessingService {

    private final Utils utils;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, CarModel> input = streamsBuilder
                .stream("cars", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> {
                    CarModel cm = utils.deserializeCarModel(v);
                    System.out.println(String.valueOf(cm.getTs()).substring(6) + "->" + cm.getBrand());
                })
                .mapValues(utils::deserializeCarModel);

        // Process each brand separately
        KStream<String, CarModel> fordStream = input.filter((key, value) -> "Ford".equals(value.getBrand()));
        KStream<String, CarModel> bmwStream = input.filter((key, value) -> "BMW".equals(value.getBrand()));
        KStream<String, CarModel> toyotaStream = input.filter((key, value) -> "Toyota".equals(value.getBrand()));

        KStream<String, CombinedDataModel> ford = processAllCars(fordStream);
        KStream<String, CombinedDataModel> bmw = processAllCars(bmwStream);
        KStream<String, CombinedDataModel> toyota = processAllCars(toyotaStream);

        KStream<String, CombinedDataModel> allCars = ford.merge(bmw).merge(toyota);

        KStream<String, AggregatedCombinedDataModel> result = findAllCars(allCars);

        result.peek((k, v) -> System.out.println("Result = " + v.getWindowS() + " " + v));
    }


    private KStream<String, CombinedDataModel> processAllCars(KStream<String, CarModel> stream) {
        return stream
                .selectKey((k, v) -> v.getBrand())
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        CombinedDataModel::new,
                        this::aggregateFunc,
                        Materialized.with(Serdes.String(), new JsonSerde<>(CombinedDataModel.class))
                )
                .toStream()
                .map((key, value) -> {
                    value.setWindowS(String.valueOf(key.window().start()));
                    value.setWindowE(String.valueOf(key.window().end()));
                    return KeyValue.pair(key.key(), value);
                });
    }

    private KStream<String, AggregatedCombinedDataModel> findAllCars(KStream<String, CombinedDataModel> allCars) {
        return allCars
                .selectKey((k, v) -> v.getWindowS())
                .groupByKey()
                .aggregate(
                        AggregatedCombinedDataModel::new,
                        this::combineAggregateFunc,
                        Materialized.with(Serdes.String(), new JsonSerde<>(AggregatedCombinedDataModel.class))
                )
                .toStream()
                .map(KeyValue::pair);
    }

    private CombinedDataModel aggregateFunc(String key, CarModel value, CombinedDataModel aggregate) {
        if (aggregate.getTotalPrice() == null) {
            aggregate.setBrand(value.getBrand());
            aggregate.setTotalPrice(value.getPrice());
        } else {
            aggregate.setTotalPrice(value.getPrice() + aggregate.getTotalPrice());
        }
        return aggregate;
    }

    private AggregatedCombinedDataModel combineAggregateFunc(String key, CombinedDataModel value, AggregatedCombinedDataModel aggregate) {
        aggregate.setWindowS(value.getWindowS().substring(6));
        aggregate.setWindowE(value.getWindowE().substring(6));
        if (value.getBrand().equals("Ford")) {
            aggregate.setTotalPriceOfFord((aggregate.getTotalPriceOfFord() != null ? aggregate.getTotalPriceOfFord() : 0) + value.getTotalPrice());
        } else if (value.getBrand().equals("BMW")) {
            aggregate.setTotalPriceOfBMW((aggregate.getTotalPriceOfBMW() != null ? aggregate.getTotalPriceOfBMW() : 0) + value.getTotalPrice());
        } else if (value.getBrand().equals("Toyota")) {
            aggregate.setTotalPriceOfToyota((aggregate.getTotalPriceOfToyota() != null ? aggregate.getTotalPriceOfToyota() : 0) + value.getTotalPrice());
        }
        return aggregate;
    }

//    private void processBrand(KStream<String, CarModel> sourceStream, String brand) {
//        sourceStream.filter((key, value) -> brand.equals(value.getBrand()))
//                .groupByKey()
//                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
//                .aggregate(
//                        () -> 0,
//                        (key, value, aggregate) -> aggregate + value.getPrice(),
//                        Materialized.with(Serdes.String(), new JsonSerde<>(CarModel.class))
//                )
//                .toStream()
//                .map((key, value) -> new KeyValue<>(key.key(), brand + ":" + value))
//                .to("aggregated-topic", Produced.with(Serdes.String(), Serdes.String()));
//    }
}

