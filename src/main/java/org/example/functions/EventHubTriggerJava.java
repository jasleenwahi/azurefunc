package org.example.functions;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;


import java.util.*;

/**
 * Azure Functions with Event Hub trigger.
 */
public class EventHubTriggerJava {


    /**
     * This function will be invoked when an event is received from Azure Event Hub.
     */
    @FunctionName("EventHubTriggerJava")
    public void run(
            @EventHubTrigger(name = "message",
                    eventHubName = "myeventhub",
                    connection = "connectionString",
                    consumerGroup = "$Default",
                    cardinality = Cardinality.MANY)
            List<Car> message,
            @CosmosDBOutput(
                    name = "updatedCarDetails",
                    databaseName = "CarFactory",
                    collectionName = "DbContainer",
                    connectionStringSetting = "ConnectionStringSetting",
                    createIfNotExists = true
            )
            OutputBinding<List<Car>> updatedCarDetails,
            final ExecutionContext context
    ) {
            List<Car> list = new ArrayList<>();
            for(Car details: message){
                context.getLogger().info("Raw Car Data : " + details);
                Double updatedMileage = TransformData.updateMileage(details.getMileage());
                Double updatedPrice = TransformData.updatePrice(details.getPrice());
                details.setMileage(updatedMileage);
                details.setPrice(updatedPrice);
                context.getLogger().info("Transformed Car Data : " + details);
                details.setCarId(details.getCarId()+1);
                list.add(details);
            }
        updatedCarDetails.setValue(list);

        }


}
