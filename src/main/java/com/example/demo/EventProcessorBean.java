package com.example.demo;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class EventProcessorBean {

    private final Map<String, EventStats> lastEventReceivedEvent = new HashMap<>();
    public EventProcessorBean(
            @Value("${azure.eventhub.connection-string:}") String eventHubConnectionString,
            @Value("${azure.eventhub.name:}") String eventHubName,
            @Value("${azure.eventhub.consumergroup:}") String consumerGroup,
            @Value("${azure.blobstorage.connection-string:}") String storageConnectionString,
            @Value("${azure.blobstorage.container-name:}") String storageContainerName) {
        BlobContainerAsyncClient blobClient = new BlobContainerClientBuilder()
                .connectionString(storageConnectionString)
                .containerName(storageContainerName)
                .buildAsyncClient();

        EventProcessorClient processor = new EventProcessorClientBuilder()
          .connectionString(eventHubConnectionString, eventHubName)
          .consumerGroup(consumerGroup)
          .checkpointStore(new BlobCheckpointStore(blobClient))
          .processEvent(this::processEvent)
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
        OffsetDateTime processorStartTime = OffsetDateTime.now();

        log.info("Processor started at " + processorStartTime);

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
            log.info("Processor running since {}. Current partition state: \n {}\n", processorStartTime,
                    lastEventReceivedEvent);
        }, 0, 1, TimeUnit.MINUTES);
    }

  private void processEvent(EventContext eventContext) {
      EventData eventData = eventContext.getEventData();
      Long sequenceNumber = eventData.getSequenceNumber();
      String partitionId = eventContext.getPartitionContext().getPartitionId();
      boolean shouldLog = sequenceNumber % 100 == 0;

      if (shouldLog) {
          log.info("Received event from partition = {}, seq num = {}", partitionId, sequenceNumber);
      }

      eventContext.updateCheckpoint();

      this.lastEventReceivedEvent.putIfAbsent(partitionId, new EventStats());
      EventStats eventStats = this.lastEventReceivedEvent.get(partitionId);
      eventStats.setEnqueuedTime(eventData.getEnqueuedTime().atOffset(ZoneOffset.UTC));
      eventStats.setProcessedTime(OffsetDateTime.now(ZoneOffset.UTC));
      eventStats.setLastSequenceNumber(eventData.getSequenceNumber());
      eventStats.incrementEventsReceivedCount();

      if (shouldLog) {
          log.info("Completed processing event partition = {}, seq num = {}", partitionId, sequenceNumber);
      }
  }

}
