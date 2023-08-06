package com.nashtech.functions.trigger;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import com.nashtech.functions.util.CarUtil;
import com.nashtech.functions.model.Car;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Azure Functions with Event Hub trigger.
 */
public class EventHubTriggerJava {


    /**
     * This function will be invoked when an event is received from Event Hub.
     */
    @FunctionName("EventHubTriggerJava")
    public void run(
            @EventHubTrigger(name = "message",
                    eventHubName = "myeventhub",
                    connection = "connectionString",
                    consumerGroup = "$Default",
                    cardinality = Cardinality.MANY)
            List<Car> carDetails,
            @CosmosDBOutput(
                    name = "updatedCarDetails",
                    databaseName = "az-car-db",
                    collectionName = "az-car-collection",
                    connectionStringSetting = "ConnectionStringSetting",
                    createIfNotExists = true
            )
            OutputBinding<List<Car>> updatedCarDetails,
            final ExecutionContext context
    ) {
        context.getLogger().info("Java Event Hub trigger function executed.");
        List<Car> collection = carDetails.stream()
                .map(car -> {
                    car.setMileage(CarUtil.updateMileage(car.getMileage()));
                    car.setPrice(CarUtil.updatePrice(car.getPrice()));
                    return car;
                }).collect(Collectors.toList());
        updatedCarDetails.setValue(collection);
    }


}