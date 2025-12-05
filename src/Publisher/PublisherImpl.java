package Publisher;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import Message.Message;

public class PublisherImpl extends UnicastRemoteObject implements Publisher 
{
    // Subscriber Parameters
    private static final long serialVersionUID = 1L;
    private final String id;

    protected PublisherImpl(String id) throws RemoteException 
    {
        super();
        this.id = id;
    }
    
    @Override
    public void sent(Message msg) throws RemoteException 
    {
        System.out.println("Publisher " + id + " sent message: " + msg.getContent());
    }
    
    @Override
    public String toString() 
    {
        return "Publisher[" + id + "]";
    }
}
