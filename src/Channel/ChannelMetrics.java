package Channel;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

public class ChannelMetrics
{
    /* Message counters */
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong messagesDelivered = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);
    private final AtomicLong messagesDropped = new AtomicLong(0);
    private final AtomicLong duplicatesRejected = new AtomicLong(0);

    /* Queue metrics */
    private final AtomicInteger currentGlobalQueueSize = new AtomicInteger(0);
    private final AtomicInteger currentPartitionCount = new AtomicInteger(0);
    private final AtomicInteger currentSubscriberCount = new AtomicInteger(0);

    /* Performance metrics */
    private final AtomicLong totalDeliveryTime = new AtomicLong(0);
    private final AtomicLong deliveryCount = new AtomicLong(0);

    /* Circuit breaker metrics */
    private final AtomicInteger circuitsOpen = new AtomicInteger(0);
    private final AtomicInteger circuitsHalfOpen = new AtomicInteger(0);

    /* Restoration metrics */
    private final AtomicLong restorationsPerformed = new AtomicLong(0);
    private final AtomicLong messagesRestored = new AtomicLong(0);

    /* Increment methods */
    public void incrementPublished() { messagesPublished.incrementAndGet(); }
    public void incrementDelivered() { messagesDelivered.incrementAndGet(); }
    public void incrementFailed() { messagesFailed.incrementAndGet(); }
    public void incrementDropped() { messagesDropped.incrementAndGet(); }
    public void incrementDuplicatesRejected() { duplicatesRejected.incrementAndGet(); }
    public void incrementRestorationsPerformed() { restorationsPerformed.incrementAndGet(); }
    public void addMessagesRestored(long count) { messagesRestored.addAndGet(count); }

    /* Update methods */
    public void updateGlobalQueueSize(int size) { currentGlobalQueueSize.set(size); }
    public void updatePartitionCount(int count) { currentPartitionCount.set(count); }
    public void updateSubscriberCount(int count) { currentSubscriberCount.set(count); }
    public void updateCircuitsOpen(int count) { circuitsOpen.set(count); }
    public void updateCircuitsHalfOpen(int count) { circuitsHalfOpen.set(count); }

    /* Record delivery time */
    public void recordDeliveryTime(long timeMs)
    {
        totalDeliveryTime.addAndGet(timeMs);
        deliveryCount.incrementAndGet();
    }

    /* Getters */
    public long getMessagesPublished() { return messagesPublished.get(); }
    public long getMessagesDelivered() { return messagesDelivered.get(); }
    public long getMessagesFailed() { return messagesFailed.get(); }
    public long getMessagesDropped() { return messagesDropped.get(); }
    public long getDuplicatesRejected() { return duplicatesRejected.get(); }
    public int getCurrentGlobalQueueSize() { return currentGlobalQueueSize.get(); }
    public int getCurrentPartitionCount() { return currentPartitionCount.get(); }
    public int getCurrentSubscriberCount() { return currentSubscriberCount.get(); }
    public int getCircuitsOpen() { return circuitsOpen.get(); }
    public int getCircuitsHalfOpen() { return circuitsHalfOpen.get(); }
    public long getRestorationsPerformed() { return restorationsPerformed.get(); }
    public long getMessagesRestored() { return messagesRestored.get(); }

    /* Average delivery time */
    public double getAverageDeliveryTimeMs()
    {
        long count = deliveryCount.get();
        if (count == 0) return 0.0;
        return (double) totalDeliveryTime.get() / count;
    }

    /* Print metrics summary */
    public String getMetricsSummary()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== Channel Metrics ==========\n");
        sb.append(String.format("Messages Published:      %d\n", messagesPublished.get()));
        sb.append(String.format("Messages Delivered:      %d\n", messagesDelivered.get()));
        sb.append(String.format("Messages Failed:         %d\n", messagesFailed.get()));
        sb.append(String.format("Messages Dropped:        %d\n", messagesDropped.get()));
        sb.append(String.format("Duplicates Rejected:     %d\n", duplicatesRejected.get()));
        sb.append(String.format("Current Global Queue:    %d\n", currentGlobalQueueSize.get()));
        sb.append(String.format("Active Partitions:       %d\n", currentPartitionCount.get()));
        sb.append(String.format("Active Subscribers:      %d\n", currentSubscriberCount.get()));
        sb.append(String.format("Circuits Open:           %d\n", circuitsOpen.get()));
        sb.append(String.format("Circuits Half-Open:      %d\n", circuitsHalfOpen.get()));
        sb.append(String.format("Restorations Performed:  %d\n", restorationsPerformed.get()));
        sb.append(String.format("Messages Restored:       %d\n", messagesRestored.get()));
        sb.append(String.format("Avg Delivery Time:       %.2f ms\n", getAverageDeliveryTimeMs()));
        sb.append("=====================================\n");
        return sb.toString();
    }

    /* Reset all metrics */
    public void reset()
    {
        messagesPublished.set(0);
        messagesDelivered.set(0);
        messagesFailed.set(0);
        messagesDropped.set(0);
        duplicatesRejected.set(0);
        totalDeliveryTime.set(0);
        deliveryCount.set(0);
        restorationsPerformed.set(0);
        messagesRestored.set(0);
    }
}
