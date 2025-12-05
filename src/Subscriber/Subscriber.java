package Subscriber;
import java.rmi.Remote;
import java.rmi.RemoteException;

import Message.Message;

public interface Subscriber extends Remote 
{
    /* Returns the ID of the subscriber */
    String getID() throws RemoteException;
    
    /* Receive message from the broker */
    boolean receive(Message msg) throws RemoteException;

    /* Receive message from the broker with added random delay */
    boolean receiveDelayed(Message msg) throws RemoteException;

    /* Receive message from the broker, but crash after a set number of messages */
    boolean receiveCrashed(Message msg) throws RemoteException;

}
