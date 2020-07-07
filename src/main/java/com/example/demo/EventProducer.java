// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.example.demo;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

public class EventProducer {

  public static void main(String args[]) throws Exception {
    String connectionString = System.getProperty("EVENT_HUBS_CONNECTION_STRING");
    EventHubProducerAsyncClient producer = new EventHubClientBuilder()
        .connectionString(connectionString)
        .buildAsyncProducerClient();
    System.out.println("Creating batch and sending events");

    getIntegerFlux()
        .delaySequence(Duration.ofMillis(2000))
        .flatMap(nextInt -> {
          if (nextInt % 10000 == 0) {
            System.out.println("test event " + nextInt);
          }
          return producer.createBatch()
              .flatMap(batch -> {
                batch.tryAdd(new EventData("test event " + nextInt));
                return producer.send(batch);
              });
        })
        .subscribe(unused -> {
              System.out.println("Message sent");
            },
            error -> System.out.println("Error sending event " + error.getMessage()),
            () -> System.out.println("Complete sending"));
    TimeUnit.DAYS.sleep(10);
    producer.close();
    System.out.println("Closing application");
  }

  private static Flux<Integer> getIntegerFlux() {
    return Flux.generate(() -> Tuples.of(0, 1),
        (state, sink) -> {
          try {
            TimeUnit.MILLISECONDS.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          sink.next(state.getT1());
          return Tuples.of(state.getT2(), state.getT2() + 1);
        });
  }


}
