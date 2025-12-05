package Broker;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Channel.Channel;
import Channel.ChannelImpl;

public class BrokerImpl extends UnicastRemoteObject implements Broker 
{
    // Broker Parameters
    private static final long serialVersionUID = 1L;
    private final Map<String, Channel> channels;

    protected BrokerImpl() throws RemoteException 
    {
        super();
        channels = new HashMap<>();
    }

    /* Create a channel with the given name if it does not already exist */
    @Override
    public synchronized Channel createChannel(String name) throws RemoteException 
    {
        if (!channels.containsKey(name)) 
        {
            Channel channel = new ChannelImpl(name);
            channels.put(name, channel);
            System.out.println("Channel created: " + name);
            return channel;
        } 
        else 
        {
            System.out.println("Channel already exists: " + name);
            return channels.get(name);
        }
    }

    /* Getter */
    @Override
    public synchronized Channel getChannel(String name) throws RemoteException 
    {
        return channels.get(name);
    }

    /* List available channels for discovery */
    @Override
    public synchronized List<String> listChannels() throws RemoteException 
    {
        return new ArrayList<>(channels.keySet());
    }
}
