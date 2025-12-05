package Broker;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import Channel.Channel;

public interface Broker extends Remote 
{
    /* Create channel */
    Channel createChannel(String name) throws RemoteException;
    
    /* Lookup channe; */
    Channel getChannel(String name) throws RemoteException;
    
    /* List channels */
    List<String> listChannels() throws RemoteException;
}
