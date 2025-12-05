package Channel;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    
    /* Mapping from partition IDs to their assigned subscriber IDs  */
    private final ConcurrentHashMap<String, String> partitionAssignments;
    
    /* Mapping from subscriber IDs to its queues */
    private final ConcurrentHashMap<String, SubscriberData> subscribers;

    /* Set of all delivered message IDs for tracking duplicates */
    private final Set<String> deliveredMessages;

    /* Lock to signal when a subscriber becomes available */
    private final Object subscriberLock = new Object();
    
    /* Offloader threads that distributes messages from queues */
    private final Thread globalOffloader;
    private final Thread partitionOffloader;

    /* Published counter */
    private volatile long publishedCount;

    /* Restoration checks */
    private volatile boolean restoring = false;

    
    /* Constructor */
    public ChannelImpl(String channelName) throws RemoteException 
    {
        super();

        /* Initialise queues and maps */
        globalQueue = new PriorityBlockingQueue<>(1000, timestampComparator);
        backupQueue = new PriorityBlockingQueue<>(1000, timestampComparator);

        partitionQueues = new ConcurrentHashMap<>();
        partitionAssignments = new ConcurrentHashMap<>();
        subscribers = new ConcurrentHashMap<>();

        deliveredMessages = new HashSet<>();
        
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
            /* Ignore new messages during restoration */
            if (restoring) 
            {
                System.out.println("Dropping " + msg.getMessageID() + " (restore in progress)");
                return;
            }
            globalQueue.offer(msg);
            backupQueue.offer(msg); /* For queue recovery */
            System.out.println("Restoring: " + msg);

            publishedCount++; /* Increment published count */

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

    /* Restore from backup, assuming all queues in the channel fail */
    public void restoreFromBackup() 
    {
        System.out.println("Restoring from backup");
        backupQueue.forEach(msg -> 
        {
            System.out.println("Restoring: " + msg);
            globalQueue.offer(msg);
        });
        System.out.println("Restoration complete");
        backupQueue.clear();
        restoring = false;
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
        PriorityBlockingQueue<Message> sQueue = new PriorityBlockingQueue<>(1000, timestampComparator);
        SubscriberData data = new SubscriberData(sub, sQueue);
        subscribers.put(id, data);
        
        /* Start a delivery thread for subscriber */
        Thread deliveryThread = new Thread(new Delivery(data));
        data.setDeliveryThread(deliveryThread);
        deliveryThread.start();

        System.out.println("Subscriber registered: " + sub.getID());

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
                partitionQueues.computeIfAbsent(partition, p -> new PriorityBlockingQueue<>(1000, timestampComparator));
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
            
    /* Returns the active subscriber currently doing the least processing (or is due to process) */
    private SubscriberData getLeastLoadedSubscriber() 
    {
        SubscriberData chosen = null;
        int minEffectiveLoad = Integer.MAX_VALUE;
        for (SubscriberData data : subscribers.values()) 
        {
            if (!data.isActive()) continue; /* Ignore inactive subscribers */
            
            /* Subscriber queue load */
            int queueLoad = data.getSubscriberQueue().size();
                
            /* Subscriber partition count */
            int partitionCount = (int) partitionAssignments
                                        .values()
                                        .stream()
                                        .filter(id -> id.equals(data.getSubscriber().toString()))
                                        .count();

            /* Effective load based on current queue and partitions of subscriber */
            int effectiveLoad = queueLoad + (partitionCount * 10);
            
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
            while (!Thread.currentThread().isInterrupted()) 
            {
                
                try
                {
                    /* Get message from globalQueue */
                    Message msg = globalQueue.take();                    
                    String partition = msg.getPartitionID();
                    
                    /* Create partition queue, if not one already */
                    partitionQueues.computeIfAbsent(partition, p -> new PriorityBlockingQueue<>(1000, timestampComparator));
                    PriorityBlockingQueue<Message> pQueue = partitionQueues.get(partition);
                    
                    /* Offer message to queue */
                    pQueue.offer(msg);
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
            while (!Thread.currentThread().isInterrupted()) 
            {
                try 
                {
                    /* Compare published to delivered, if not equal and the global queue is empty but the backup is not, the queues have crashed */
                    int inProcessing = (int) (publishedCount - deliveredMessages.size());
                    
                    if (inProcessing > 0) 
                    {
                        if (globalQueue.isEmpty() && partitionQueues.isEmpty() && !backupQueue.isEmpty())
                        {
                            synchronized (subscriberLock) 
                            {
                                /* If not, prevent new data from being added temporarily, and restore backup */
                                subscriberLock.wait(1000);
                                restoring = true;
                                restoreFromBackup();
                            }
                            continue;
                        }
                    }
                    if (restoring) continue;

                    /* Flag to check if partitions are being assigned */
                    boolean active = false;

                    /* Distribute partition queue messages to subscribers */
                    for (String partition : partitionQueues.keySet()) 
                    {
                        /* Get partition queue */
                        PriorityBlockingQueue<Message> pQueue = partitionQueues.get(partition);

                        /* If nothing to process, continue */
                        if (pQueue == null || pQueue.isEmpty()) continue;

                        /* Get assigned subcriber */
                        SubscriberData target = assignSubscriber(partition);

                        /* If no target, continue */
                        if (target == null) continue;

                        /* Offload message to a subscriber queue */
                        Message msg = pQueue.take();
                        if (msg != null) 
                        {
                            target.getSubscriberQueue().offer(msg);
                            active = true;
                        }
                    }

                    /* Pause thread temporarily if nothing is being processed */
                    if (!active) 
                    {
                        synchronized (subscriberLock) 
                        {
                            subscriberLock.wait(1000); 
                        }
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

        private static final int MAX_DELIVERY_ATTEMPTS = 3;
        private static final int MAX_DELIVERY_DELAYS = 3;
        private static final long MAX_DELIVERY_DELAY = 3000; /* 3000ms */
        private static final long MAX_DELIVERY_INTERRUPT = 300; /* 300ms */
        private int delays = 0;
        
        public Delivery(SubscriberData data) { this.data = data; }
        
        @Override
        public void run() 
        {
            while (data.isActive()) 
            {
                try 
                {
                    /* Pop head of subscriber queue */
                    Message msg = data.getSubscriberQueue().take();
                    int attempts = 0;

                    boolean delivered = deliveredMessages.contains(msg.getMessageID()); /* True if already delivered */
    
                    if (delivered) continue; /* No need to send again */

                    /* Else, attempt to send message until success or queue crash alert */
                    long startTime = System.currentTimeMillis();
                    while (attempts < MAX_DELIVERY_ATTEMPTS && !delivered) 
                    {
                        attempts++;
                        try 
                        {
                            /* Send the message to the subscriber, with acknowledgment */
                            delivered = data.getSubscriber().receive(msg); 
                            
                            /* Add to delivered if successful */
                            deliveredMessages.add(msg.getMessageID());

                            /* If so, remove from backup */
                            if (delivered) backupQueue.remove(msg);
                        } 
                        catch (RemoteException e) 
                        {
                            /* Add directly to original partition queue, as subscriber is already unsubscribed if this exception is reached */
                            partitionQueues.get(msg.getPartitionID()).offer(msg);
                            deliveredMessages.remove(msg.getMessageID());
                            System.err.println("RE delivering message " + msg.getMessageID() + " to subscriber: " + data.getSubscriber() + " [Attempt " + attempts + "]");
                            /* Pause and retry */
                            Thread.sleep(500);
                        }

                        /* Calculate time to receive */
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        if (elapsedTime > MAX_DELIVERY_DELAY) delays++;
                        else if (elapsedTime > MAX_DELIVERY_INTERRUPT) System.out.println("Temporary Interrupt");

                    }

                    if (delays >= MAX_DELIVERY_DELAYS) 
                    {
                        /* Mark as slow if all attempts fail */
                        System.err.println("Reassigning events of Subscriber " + data.getSubscriber().getID() + " after " + MAX_DELIVERY_DELAYS + " delays");
                        /* Reassign its work (i.e., unsubscribe and re-subscribe) */
                        delays = 0;
                        unsubscribe(data.getSubscriber()); 
                        subscribe(data.getSubscriber());
                    }
    
                    if (!delivered) 
                    {
                        data.getSubscriberQueue().offer(msg); /* Add to head of queue */
                        deliveredMessages.remove(msg.getMessageID()); /* Remove from the delilvered set */
                        /* Mark as crashed if all attempts fail */
                        System.err.println("Removing Subscriber " + data.getSubscriber().getID() + " after " + MAX_DELIVERY_ATTEMPTS + " attempts");
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
    
    /* Helper class for subscriber data */
    private static class SubscriberData 
    {
        private final Subscriber subscriber;
        private PriorityBlockingQueue<Message> subscriberQueue;

        private volatile boolean active;
        private Thread deliveryThread;
        
        public SubscriberData(Subscriber subscriber, PriorityBlockingQueue<Message> subscriberQueue) 
        {
            this.subscriber = subscriber;
            this.subscriberQueue = subscriberQueue;
            this.active = true;
        }
        
        /* Getters */
        public Subscriber getSubscriber() { return subscriber; }
        
        public PriorityBlockingQueue<Message> getSubscriberQueue() { return subscriberQueue; }
        
        /* Setters */
        public void setDeliveryThread(Thread t) { this.deliveryThread = t; }
        
        /* Activity control */
        public boolean isActive() { return active; }
        
        public void stop() 
        {
            active = false;
            if (deliveryThread != null) 
            {
                deliveryThread.interrupt();
            }
        }
    }
}
