package com.example.demo;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EventProcessorBean {

    public EventProcessorBean(
            @Value("${azure.eventhub.connection-string:}") String eventHubConnectionString,
            @Value("${azure.eventhub.name:}") String eventHubName,
            @Value("${azure.blobstorage.connection-string:}") String storageConnectionString,
            @Value("${azure.blobstorage.container-name:}") String storageContainerName) {
        BlobContainerAsyncClient blobClient = new BlobContainerClientBuilder()
                .connectionString(storageConnectionString)
                .containerName(storageContainerName)
                .buildAsyncClient();

      EventProcessorClient processor = new EventProcessorClientBuilder()
          .connectionString(eventHubConnectionString, eventHubName)
          .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
          .checkpointStore(new BlobCheckpointStore(blobClient))
          .processEvent(EventProcessorBean::processEvent)
//          .processEventBatch(EventProcessorBean::processEventBatch, 100, Duration.ofSeconds(3))
          .processError(context -> {
            log.error("Error occurred on partition: {}.",
                context.getPartitionContext().getPartitionId());
          })
          .processPartitionInitialization(initializationContext -> {
            log.info("Started receiving on partition: {}",
                initializationContext.getPartitionContext().getPartitionId());
          })
          .processPartitionClose(closeContext -> {
            log.info("Stopped receiving on partition: {}. Reason: {}",
                closeContext.getPartitionContext().getPartitionId(),
                closeContext.getCloseReason());

          })
          .buildEventProcessorClient();

        processor.start();
        log.info("Processor started");
    }

  private static void processEvent(EventContext eventContext) {
    if (eventContext.getEventData() != null && eventContext.getEventData().getSequenceNumber() % 100 == 0) {
      System.out.println(
          String.format("Received event from partition = %s, seq num = %d, offset = %d on thread = %s and "
                  + "thread group = %s, %s",
              eventContext.getPartitionContext().getPartitionId(),
              eventContext.getEventData().getSequenceNumber(),
              eventContext.getEventData().getOffset(),
              Thread.currentThread().getClass().getName(),
              Thread.currentThread().getThreadGroup().getName(),
              Thread.currentThread().getName()));
      eventContext.updateCheckpoint();
    }
  }

  private static void processEventBatch(EventBatchContext eventBatchContext) {
    System.out.println(String.format("Received event batch from partition = %s, seq num = %d on thread type= %s "
            + "and thread group = %s, %s",
        eventBatchContext.getPartitionContext().getPartitionId(),
        eventBatchContext.getEvents().get(0).getSequenceNumber(),
        Thread.currentThread().getClass().getName(),
        Thread.currentThread().getThreadGroup().getName(),
        Thread.currentThread().getName()));
    eventBatchContext.updateCheckpoint();
  }

}
