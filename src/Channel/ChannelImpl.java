package Channel;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

import Message.Message;
import Subscriber.Subscriber;

public class ChannelImpl extends UnicastRemoteObject implements Channel 
{
    
    /* Ordering of the queues, most recent timestamps are processed first */
    private static final Comparator<Message> timestampComparator = 
        (m1, m2) -> m1.getTimestamp().compareTo(m2.getTimestamp());

    /* Offloading queues */
    private final PriorityBlockingQueue<Message> globalQueue;
    private final PriorityBlockingQueue<Message> backupQueue;

    /* Mapping from partition IDs to their queues */
    private final ConcurrentHashMap<String, PriorityBlockingQueue<Message>> partitionQueues;

    /* Queue of partition IDs with pending messages - more efficient than iterating all partitions */
    private final ConcurrentLinkedQueue<String> activePartitions;

    /* Mapping from partition IDs to their assigned subscriber IDs  */
    private final ConcurrentHashMap<String, String> partitionAssignments;
    
    /* Mapping from subscriber IDs to its queues */
    private final ConcurrentHashMap<String, SubscriberData> subscribers;

    /* Set of all delivered message IDs for tracking duplicates - using ConcurrentHashMap for thread-safe LRU behavior */
    private final ConcurrentHashMap<String, Long> deliveredMessages;

    /* Lock to signal when a subscriber becomes available */
    private final Object subscriberLock = new Object();

    /* Lock for restoration operations to prevent race conditions */
    private final Object restorationLock = new Object();

    /* Metrics tracking */
    private final ChannelMetrics metrics;
    
    /* Offloader threads that distributes messages from queues */
    private final Thread globalOffloader;
    private final Thread partitionOffloader;

    /* Published counter */
    private volatile long publishedCount;

    /* Restoration checks */
    private volatile boolean restoring = false;

    /* Shutdown flag */
    private volatile boolean shuttingDown = false;

    
    /* Constructor */
    public ChannelImpl(String channelName) throws RemoteException 
    {
        super();

        /* Initialise queues and maps */
        globalQueue = new PriorityBlockingQueue<>(ChannelConfig.QUEUE_CAPACITY, timestampComparator);
        backupQueue = new PriorityBlockingQueue<>(ChannelConfig.QUEUE_CAPACITY, timestampComparator);

        partitionQueues = new ConcurrentHashMap<>();
        activePartitions = new ConcurrentLinkedQueue<>();
        partitionAssignments = new ConcurrentHashMap<>();
        subscribers = new ConcurrentHashMap<>();

        deliveredMessages = new ConcurrentHashMap<>();

        /* Initialize metrics */
        metrics = new ChannelMetrics();

        /* Start threads */
        globalOffloader = new Thread(new GlobalOffloader());
        globalOffloader.start();

        partitionOffloader = new Thread(new PartitionOffloader());
        partitionOffloader.start();

        /* Intialise published counter */
        publishedCount = 0;
    }


    /* Publish the message, adding it to the global queue*/
    @Override
    public void publish(Message msg) throws RemoteException
    {
        try
        {
            synchronized (restorationLock)
            {
                /* Ignore new messages during restoration */
                if (restoring)
                {
                    System.out.println("Dropping " + msg.getMessageID() + " (restore in progress)");
                    return;
                }
                globalQueue.offer(msg);
                backupQueue.offer(msg); /* For queue recovery */
                System.out.println("Published: " + msg);

                publishedCount++; /* Increment published count */
                metrics.incrementPublished();
                metrics.updateGlobalQueueSize(globalQueue.size());
            }
        }
        catch (Exception e)
        {
            System.err.println("Error publishing message: " + e.getMessage());
            throw new RemoteException("Publish error", e);
        }
    }

    /* Kill queues, for testing backup */
    public void killQueues()
    {
        synchronized (restorationLock)
        {
            /* Clear queues */
            restoring = true;
            globalQueue.clear();
            partitionQueues.clear();

            for (SubscriberData data : subscribers.values())
            {
                data.getSubscriberQueue().clear();
            }

            System.out.println("All queues cleared");
        }
    }

    /* Restore from backup, assuming all queues in the channel fail */
    public void restoreFromBackup()
    {
        synchronized (restorationLock)
        {
            System.out.println("Restoring from backup");
            int restoredCount = 0;
            for (Message msg : backupQueue)
            {
                System.out.println("Restoring: " + msg);
                globalQueue.offer(msg);
                restoredCount++;
            }
            System.out.println("Restoration complete");
            metrics.incrementRestorationsPerformed();
            metrics.addMessagesRestored(restoredCount);
            backupQueue.clear();
            restoring = false;
        }
    }

    /* Get metrics summary */
    public String getMetrics()
    {
        metrics.updatePartitionCount(partitionQueues.size());
        metrics.updateGlobalQueueSize(globalQueue.size());

        /* Count circuit states */
        int openCircuits = 0;
        int halfOpenCircuits = 0;
        for (SubscriberData data : subscribers.values())
        {
            CircuitState state = data.getCircuitState();
            if (state == CircuitState.OPEN) openCircuits++;
            else if (state == CircuitState.HALF_OPEN) halfOpenCircuits++;
        }
        metrics.updateCircuitsOpen(openCircuits);
        metrics.updateCircuitsHalfOpen(halfOpenCircuits);

        return metrics.getMetricsSummary();
    }

    /* Graceful shutdown */
    public void shutdown()
    {
        shuttingDown = true;
        System.out.println("Initiating graceful shutdown...");

        /* Stop accepting new messages */
        synchronized (restorationLock)
        {
            restoring = true;
        }

        /* Wait for queues to drain */
        long shutdownStart = System.currentTimeMillis();
        long timeout = 30000; /* 30 seconds timeout */

        while (!globalQueue.isEmpty() || !partitionQueues.isEmpty())
        {
            if (System.currentTimeMillis() - shutdownStart > timeout)
            {
                System.err.println("Shutdown timeout reached, forcing shutdown");
                break;
            }

            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
        }

        /* Stop all subscriber threads */
        for (SubscriberData data : subscribers.values())
        {
            data.stop();
        }

        /* Interrupt offloader threads */
        if (globalOffloader != null)
        {
            globalOffloader.interrupt();
        }
        if (partitionOffloader != null)
        {
            partitionOffloader.interrupt();
        }

        /* Wait for threads to finish */
        try
        {
            if (globalOffloader != null)
            {
                globalOffloader.join(5000);
            }
            if (partitionOffloader != null)
            {
                partitionOffloader.join(5000);
            }
        }
        catch (InterruptedException e)
        {
            System.err.println("Interrupted while waiting for threads to finish");
            Thread.currentThread().interrupt();
        }

        System.out.println("Shutdown complete");
        System.out.println(getMetrics());
    }

    /* Subscriber registration. Each subscriber gets a combined queue for all partitions assigned to it */
    @Override
    public void subscribe(Subscriber sub) throws RemoteException 
    {
        String id = sub.toString();
        if (subscribers.containsKey(id)) 
        {

            SubscriberData subscriberID = subscribers.get(id);
            if (!subscriberID.isActive()) 
            {
                /* If subscriber is inactive, remove, add again */
                unsubscribe(sub);
                System.out.println("Removed inactive subscriber: " + sub.getID());
            } 
            else
            {
                /* If subscriber exists, ignore */
                System.out.println("Subscriber already registered: " + sub.getID());
                return;
            }
        }

        /* Create a queue for the subscriber (combined messages from all its partitions) */
        PriorityBlockingQueue<Message> sQueue = new PriorityBlockingQueue<>(ChannelConfig.QUEUE_CAPACITY, timestampComparator);
        SubscriberData data = new SubscriberData(sub, sQueue);
        subscribers.put(id, data);

        /* Start a delivery thread for subscriber */
        Thread deliveryThread = new Thread(new Delivery(data));
        data.setDeliveryThread(deliveryThread);
        deliveryThread.start();

        System.out.println("Subscriber registered: " + sub.getID());
        metrics.updateSubscriberCount(subscribers.size());

        /* Notify offloader that a subscriber is available (so that partition assignments can be made) */
        synchronized (subscriberLock) 
        { 
            subscriberLock.notifyAll(); 
        }
    }

    /* Unsubscribe and stop its delivery thread */
    @Override
    public void unsubscribe(Subscriber sub) throws RemoteException 
    {
        String id = sub.toString(); /* Remove based on id */
        SubscriberData data = subscribers.remove(id);
        if (data != null) 
        {
            data.stop();
            System.out.println("Subscriber unregistered: " + sub.getID());
            
            /* Remove any partition assignment that was assigned to this subscriber */
            partitionAssignments.forEach((partition, subscriberID) -> 
            {
                if (subscriberID.equals(id)) 
                {
                    partitionAssignments.remove(partition);
                    System.out.println("Removed partition assignment for partition: " + partition);
                }
            });

            /* Anything in its queue is re-placed into the original partition queues */
            List<Message> messages = new ArrayList<>();
            data.getSubscriberQueue().drainTo(messages);
            for (Message msg : messages) 
            {
                String partition = msg.getPartitionID();
                partitionQueues.computeIfAbsent(partition, p -> new PriorityBlockingQueue<>(ChannelConfig.QUEUE_CAPACITY, timestampComparator));
                partitionQueues.get(partition).offer(msg);
                assignSubscriber(partition);
            }
        }
    }

    /* Assign a subscriber to a partition */
    private SubscriberData assignSubscriber(String partition) 
    {
        /* If there is an assigned subscriber, get it */
        String assignedSubscriber = partitionAssignments.get(partition);
        SubscriberData target = (assignedSubscriber != null)
            ? subscribers.get(assignedSubscriber)
            : null;
    
        /* If the partition is unassigned or the assigned subscriber is inactive, reassign partition */
        if (target == null || !target.isActive()) 
        {
            target = getLeastLoadedSubscriber();
            if (target != null) 
            {
                partitionAssignments.put(partition, target.getSubscriber().toString());
            }
        }
        return target;
    }
            
    /* Add delivered message with LRU eviction to prevent memory leak */
    private void addDeliveredMessage(String messageId)
    {
        long timestamp = System.currentTimeMillis();
        deliveredMessages.put(messageId, timestamp);

        /* Evict old entries if exceeds limit */
        if (deliveredMessages.size() > ChannelConfig.MAX_DELIVERED_MESSAGES)
        {
            String oldestKey = deliveredMessages.entrySet().stream()
                .min((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
                .map(e -> e.getKey())
                .orElse(null);
            if (oldestKey != null)
            {
                deliveredMessages.remove(oldestKey);
            }
        }
    }

    /* Check if message is duplicate */
    private boolean isDuplicate(String messageId)
    {
        return deliveredMessages.containsKey(messageId);
    }

    /* Handle successful message delivery */
    private void handleSuccessfulDelivery(Message msg, SubscriberData data, long startTime)
    {
        addDeliveredMessage(msg.getMessageID());
        backupQueue.remove(msg);
        data.recordSuccess();
        metrics.incrementDelivered();
        long deliveryTime = System.currentTimeMillis() - startTime;
        metrics.recordDeliveryTime(deliveryTime);
    }

    /* Handle failed message delivery */
    private void handleFailedDelivery(Message msg, SubscriberData data, int attempts)
    {
        data.recordFailure();
        metrics.incrementFailed();
        partitionQueues.get(msg.getPartitionID()).offer(msg);
        System.err.println("Failed delivering message " + msg.getMessageID() +
            " to subscriber: " + data.getSubscriber() + " [Attempt " + attempts + "]");
    }

    /* Attempt to deliver a message with retries and circuit breaker check */
    private boolean attemptDelivery(Message msg, SubscriberData data, int delays)
    {
        long startTime = System.currentTimeMillis();
        int attempts = 0;
        boolean delivered = false;

        while (attempts < ChannelConfig.MAX_DELIVERY_ATTEMPTS && !delivered)
        {
            attempts++;
            try
            {
                /* Check circuit breaker state */
                if (data.getCircuitState() == CircuitState.OPEN)
                {
                    System.err.println("Circuit breaker OPEN for subscriber: " +
                        data.getSubscriber().getID() + ", skipping delivery");
                    partitionQueues.get(msg.getPartitionID()).offer(msg);
                    return false;
                }

                /* Send the message to the subscriber */
                delivered = data.getSubscriber().receive(msg);

                /* Handle successful delivery */
                if (delivered)
                {
                    handleSuccessfulDelivery(msg, data, startTime);
                }
            }
            catch (RemoteException e)
            {
                /* Handle failed delivery */
                handleFailedDelivery(msg, data, attempts);

                /* Pause and retry */
                try
                {
                    Thread.sleep(ChannelConfig.RETRY_DELAY_MS);
                }
                catch (InterruptedException ie)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            /* Check for delays */
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > ChannelConfig.MAX_DELIVERY_DELAY_MS)
            {
                delays++;
            }
            else if (elapsedTime > ChannelConfig.MAX_DELIVERY_INTERRUPT_MS)
            {
                System.out.println("Temporary Interrupt");
            }
        }

        return delivered;
    }

    /* Returns the active subscriber currently doing the least processing (or is due to process) */
    private SubscriberData getLeastLoadedSubscriber()
    {
        SubscriberData chosen = null;
        double minEffectiveLoad = Double.MAX_VALUE;

        for (SubscriberData data : subscribers.values())
        {
            if (!data.isActive()) continue; /* Ignore inactive subscribers */

            /* Calculate load score components */
            int queueLoad = data.getSubscriberQueue().size();

            int partitionCount = (int) partitionAssignments
                                        .values()
                                        .stream()
                                        .filter(id -> id.equals(data.getSubscriber().toString()))
                                        .count();

            /* Circuit breaker penalty */
            double circuitPenalty = 0.0;
            CircuitState state = data.getCircuitState();
            if (state == CircuitState.HALF_OPEN)
            {
                circuitPenalty = 50.0; /* Moderate penalty for half-open circuits */
            }
            else if (state == CircuitState.OPEN)
            {
                continue; /* Skip completely if circuit is open */
            }

            /* Failure rate penalty */
            double failurePenalty = data.getFailureCount() * 5.0;

            /* Calculate effective load with weighted components */
            double effectiveLoad = queueLoad
                                 + (partitionCount * ChannelConfig.PARTITION_WEIGHT)
                                 + circuitPenalty
                                 + failurePenalty;

            /* Choose the subscriber with the lowest effective load */
            if (effectiveLoad < minEffectiveLoad)
            {
                minEffectiveLoad = effectiveLoad;
                chosen = data;
            }
        }
        return chosen;
    }    

    /// GlobalOffloader Thread ///
    /* Takes messages from globalQueue, enqueues the partitions */
    private class GlobalOffloader implements Runnable 
    {
        @Override
        public void run()
        {
            while (!Thread.currentThread().isInterrupted() && !shuttingDown)
            {

                try
                {
                    /* Get message from globalQueue */
                    Message msg = globalQueue.take();
                    String partition = msg.getPartitionID();

                    /* Create partition queue, if not one already */
                    boolean isNewPartition = !partitionQueues.containsKey(partition);
                    partitionQueues.computeIfAbsent(partition, p -> new PriorityBlockingQueue<>(ChannelConfig.QUEUE_CAPACITY, timestampComparator));
                    PriorityBlockingQueue<Message> pQueue = partitionQueues.get(partition);

                    /* Offer message to queue */
                    boolean wasEmpty = pQueue.isEmpty();
                    pQueue.offer(msg);

                    /* Add to active partitions queue if newly active */
                    if (isNewPartition || wasEmpty)
                    {
                        activePartitions.offer(partition);
                    }
                }
                catch (InterruptedException e) 
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /// PartitionOffloader Thread ///
    /* Iterates over each partition queue and assigns it to a subscriber */
    private class PartitionOffloader implements Runnable 
    {
        @Override
        public void run()
        {
            while (!Thread.currentThread().isInterrupted() && !shuttingDown)
            {
                try
                {
                    /* Check if restoration is needed */
                    synchronized (restorationLock)
                    {
                        /* Compare published to delivered, if not equal and the global queue is empty but the backup is not, the queues have crashed */
                        int inProcessing = (int) (publishedCount - deliveredMessages.size());

                        if (inProcessing > 0)
                        {
                            if (globalQueue.isEmpty() && partitionQueues.isEmpty() && !backupQueue.isEmpty())
                            {
                                /* Prevent new data from being added temporarily, and restore backup */
                                restoreFromBackup();
                                continue;
                            }
                        }
                        if (restoring) continue;
                    }

                    /* Process active partitions using queue for efficiency */
                    String partition = activePartitions.poll();

                    if (partition == null)
                    {
                        /* No active partitions, wait for work */
                        synchronized (subscriberLock)
                        {
                            subscriberLock.wait(ChannelConfig.SUBSCRIBER_LOCK_TIMEOUT_MS);
                        }
                        continue;
                    }

                    /* Get partition queue */
                    PriorityBlockingQueue<Message> pQueue = partitionQueues.get(partition);

                    /* If nothing to process, continue */
                    if (pQueue == null || pQueue.isEmpty())
                    {
                        continue;
                    }

                    /* Get assigned subscriber */
                    SubscriberData target = assignSubscriber(partition);

                    /* If no target, re-add partition to active queue and wait */
                    if (target == null)
                    {
                        activePartitions.offer(partition);
                        synchronized (subscriberLock)
                        {
                            subscriberLock.wait(ChannelConfig.SUBSCRIBER_LOCK_TIMEOUT_MS);
                        }
                        continue;
                    }

                    /* Offload messages in batches for efficiency */
                    int batchCount = 0;
                    while (batchCount < ChannelConfig.BATCH_SIZE && !pQueue.isEmpty())
                    {
                        Message msg = pQueue.poll();
                        if (msg != null)
                        {
                            target.getSubscriberQueue().offer(msg);
                            batchCount++;
                        }
                        else
                        {
                            break;
                        }
                    }

                    /* Re-add partition if still has messages */
                    if (!pQueue.isEmpty())
                    {
                        activePartitions.offer(partition);
                    }
                } 
                catch (InterruptedException ie) 
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }   

    /* Delivery thread for a subscriber */
    private class Delivery implements Runnable
    {
        private final SubscriberData data;
        private int delays = 0;
        
        public Delivery(SubscriberData data) { this.data = data; }
        
        @Override
        public void run()
        {
            while (data.isActive() && !shuttingDown)
            {
                try 
                {
                    /* Pop head of subscriber queue */
                    Message msg = data.getSubscriberQueue().take();

                    /* Check for duplicates */
                    if (isDuplicate(msg.getMessageID()))
                    {
                        metrics.incrementDuplicatesRejected();
                        continue; /* No need to send again */
                    }

                    /* Attempt to deliver message */
                    boolean delivered = attemptDelivery(msg, data, delays);

                    if (delays >= ChannelConfig.MAX_DELIVERY_DELAYS)
                    {
                        /* Mark as slow if all attempts fail */
                        System.err.println("Reassigning events of Subscriber " + data.getSubscriber().getID() + " after " + ChannelConfig.MAX_DELIVERY_DELAYS + " delays");
                        /* Reassign its work (i.e., unsubscribe and re-subscribe) */
                        delays = 0;
                        unsubscribe(data.getSubscriber()); 
                        subscribe(data.getSubscriber());
                    }
    
                    if (!delivered) 
                    {
                        data.getSubscriberQueue().offer(msg); /* Add to head of queue */
                        /* Remove from the delivered set */
                        /* Mark as crashed if all attempts fail */
                        System.err.println("Removing Subscriber " + data.getSubscriber().getID() + " after " + ChannelConfig.MAX_DELIVERY_ATTEMPTS + " attempts");
                        /* Close thread, reassign messages */
                        unsubscribe(data.getSubscriber());
                    }
                }
                catch (InterruptedException | RemoteException e) 
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
    
    /* Circuit breaker states */
    private enum CircuitState { CLOSED, OPEN, HALF_OPEN }

    /* Helper class for subscriber data */
    private static class SubscriberData
    {
        private final Subscriber subscriber;
        private PriorityBlockingQueue<Message> subscriberQueue;

        private volatile boolean active;
        private Thread deliveryThread;

        /* Circuit breaker fields */
        private volatile CircuitState circuitState;
        private volatile int failureCount;
        private volatile long lastFailureTime;
        
        public SubscriberData(Subscriber subscriber, PriorityBlockingQueue<Message> subscriberQueue)
        {
            this.subscriber = subscriber;
            this.subscriberQueue = subscriberQueue;
            this.active = true;
            this.circuitState = CircuitState.CLOSED;
            this.failureCount = 0;
            this.lastFailureTime = 0;
        }
        
        /* Getters */
        public Subscriber getSubscriber() { return subscriber; }

        public PriorityBlockingQueue<Message> getSubscriberQueue() { return subscriberQueue; }

        public int getFailureCount() { return failureCount; }
        
        /* Setters */
        public void setDeliveryThread(Thread t) { this.deliveryThread = t; }
        
        /* Activity control */
        public boolean isActive()
        {
            updateCircuitState();
            return active && circuitState != CircuitState.OPEN;
        }

        public void stop()
        {
            active = false;
            if (deliveryThread != null)
            {
                deliveryThread.interrupt();
            }
        }

        /* Circuit breaker methods */
        public void recordFailure()
        {
            failureCount++;
            lastFailureTime = System.currentTimeMillis();
            if (failureCount >= ChannelConfig.CIRCUIT_FAILURE_THRESHOLD)
            {
                circuitState = CircuitState.OPEN;
                try
                {
                    System.out.println("Circuit OPEN for subscriber: " + subscriber.getID());
                }
                catch (RemoteException e)
                {
                    System.err.println("Error getting subscriber ID: " + e.getMessage());
                }
            }
        }

        public void recordSuccess()
        {
            failureCount = 0;
            if (circuitState == CircuitState.HALF_OPEN)
            {
                circuitState = CircuitState.CLOSED;
                try
                {
                    System.out.println("Circuit CLOSED for subscriber: " + subscriber.getID());
                }
                catch (RemoteException e)
                {
                    System.err.println("Error getting subscriber ID: " + e.getMessage());
                }
            }
        }

        private void updateCircuitState()
        {
            long now = System.currentTimeMillis();
            if (circuitState == CircuitState.OPEN)
            {
                if (now - lastFailureTime > ChannelConfig.CIRCUIT_TIMEOUT_MS)
                {
                    circuitState = CircuitState.HALF_OPEN;
                    try
                    {
                        System.out.println("Circuit HALF_OPEN for subscriber: " + subscriber.getID());
                    }
                    catch (RemoteException e)
                    {
                        System.err.println("Error getting subscriber ID: " + e.getMessage());
                    }
                }
            }
            else if (circuitState == CircuitState.HALF_OPEN)
            {
                if (now - lastFailureTime > ChannelConfig.CIRCUIT_HALF_OPEN_TIMEOUT_MS)
                {
                    /* Reset after timeout */
                    failureCount = 0;
                }
            }
        }

        public CircuitState getCircuitState() { return circuitState; }
    }
}
