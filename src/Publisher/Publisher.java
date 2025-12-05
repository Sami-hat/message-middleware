package Publisher;
import java.rmi.Remote;
import java.rmi.RemoteException;

import Message.Message;

public interface Publisher extends Remote 
{
    /* Sent message to the broker */
    void sent(Message msg) throws RemoteException;
}
