package com.example.kafkaprocesserapp.utils;

import com.example.kafkaprocesserapp.model.CarModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class Utils {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public CarModel deserializeCarModel(String jsonString) {
        try {
            return objectMapper.readValue(jsonString, CarModel.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
