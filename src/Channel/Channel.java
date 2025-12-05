package Channel;
import java.rmi.Remote;
import java.rmi.RemoteException;

import Message.Message;
import Subscriber.Subscriber;

public interface Channel extends Remote 
{
    /* Publish to channel */
    void publish(Message msg) throws RemoteException;
    
    /* Add subscriber */
    void subscribe(Subscriber sub) throws RemoteException;
    
    /* Remove subscriber */
    void unsubscribe(Subscriber sub) throws RemoteException;

    /* Kill queues */
    void killQueues() throws RemoteException;
}
