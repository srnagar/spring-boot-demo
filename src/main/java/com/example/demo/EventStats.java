package com.example.demo;

import java.time.OffsetDateTime;

public class EventStats {
    private Long lastSequenceNumber;
    private OffsetDateTime enqueuedTime;
    private long totalEventsReceived;
    private OffsetDateTime processedTime;

    void incrementEventsReceivedCount() {
        this.totalEventsReceived++;
    }

    void setProcessedTime(OffsetDateTime processedTime) {
        this.processedTime = processedTime;
    }

    void setLastSequenceNumber(Long lastSequenceNumber) {
        this.lastSequenceNumber = lastSequenceNumber;
    }

    void setEnqueuedTime(OffsetDateTime enqueuedTime) {
        this.enqueuedTime = enqueuedTime;
    }

    @Override
    public String toString() {
        return "EventStats{" +
                "lastSequenceNumber='" + lastSequenceNumber + '\'' +
                ", enqueuedTime=" + enqueuedTime +
                ", processedAt=" + processedTime +
                ", totalEventsReceived=" + totalEventsReceived +
                "}}\n";
    }
}
