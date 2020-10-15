# Project

This is a simple maven project that receives events from Event Hub using the `EventProcessorClient`. The application
receives events from all partitions and logs a message after every 100 messages received from a partition. This
application also periodically logs the status of the processor by printing the time at which last event it processed
was sent to the Event Hub service. Note that this application does not send any events. So, a separate producer is
expected to send events to the Event Hub.
 
# Setup

## Update application properties

In `src/main/resources/application.properties`, update the application properties

```
azure.eventhub.connection-string: <your-eventhubs-connection-string>
azure.eventhub.name: <eventhub-name>
azure.eventhub.consumergroup: <consumer-group>
azure.blobstorage.connection-string: <your-storage-container-connection-string>
azure.blobstorage.container-name: <your-storage-container-name>
```
**NOTE:** Run a single instance of the processor for a consumer group so that one processor will process events from
 all partitions. If you want to run more than one instance, please use unique consumer group names for each instance.

# Run

Go to `src/main/java/com/example/demo/DemoApplication.java` and run the application in **debug mode**. It is
 important to start the application in debug mode to enable adding a breakpoint to investigate any issues.


