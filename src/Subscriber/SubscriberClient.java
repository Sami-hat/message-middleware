package Subscriber;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Broker.Broker;
import Channel.Channel;

public class SubscriberClient 
{

    // Broker constants
    private static final String BROKER_URL = "rmi://localhost/MessageBroker";
    private static final String[] CHANNEL_NAMES = {"C1", "C2"};
    private static final String[] SUB_NAMES = {"S1", "S2"};

    public static void main(String[] args) 
    {
        try 
        {
            /* Lookup the broker */
            Broker broker = (Broker) Naming.lookup(BROKER_URL);

            /* Map subscriptions to each channel */
            Map<Channel, List<Subscriber>> subscriptions = new HashMap<>();     


            /* Lookup channels */
            System.out.println("Available channels: " + broker.listChannels());
            
            /* Add each subscriber to each channel */
            for (String subName : SUB_NAMES) 
            {
                for (String channelName : CHANNEL_NAMES) 
                {
                    Channel tempChannel = broker.getChannel(channelName);
                    final Channel channel;
                    if (tempChannel == null) 
                    {
                        channel = broker.createChannel(channelName);
                    } 
                    else
                    {
                        channel = tempChannel;
                    }
    
                    /* Create a subscriber instance */
                    Subscriber sub = new SubscriberImpl(subName + "-" + channelName, channel);
                    channel.subscribe(sub);
                    /* Add to the map of channels and their list of subscribers */
                    subscriptions.computeIfAbsent(channel, k -> new ArrayList<>()).add(sub);
                }
            }

            /* Shutdown hook: On client disconnect unsubscribe all subscribers from all channels */
            Runtime.getRuntime().addShutdownHook(new Thread(() -> 
            {
                for (Map.Entry<Channel, List<Subscriber>> entry : subscriptions.entrySet()) 
                {
                    Channel channel = entry.getKey();
                    for (Subscriber subscriber : entry.getValue()) 
                    {
                        try 
                        {
                            channel.unsubscribe(subscriber);
                            System.out.println("Unsubscribed " + subscriber.toString());
                        } 
                        catch (RemoteException e) 
                        {
                            if (e.getCause() instanceof java.net.ConnectException ||
                            (e.getMessage() != null && e.getMessage().contains("Connection refused"))) 
                            {
                                System.err.println("Channel already closed when trying to unsubscribe " + subscriber.toString());
                            } 
                            else 
                            {
                                e.printStackTrace();
                            }
                        }
                    }
                }

            }));

            /* Keep the client running so that it continues to receive messages */
            while (true) 
            {
                Thread.sleep(1000);
            }
        } 
        catch (Exception e) 
        {
            System.err.println("Subscriber error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}