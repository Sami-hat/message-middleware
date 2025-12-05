package Subscriber;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;

import Message.Message;
import Channel.Channel;

public class SubscriberImpl extends UnicastRemoteObject implements Subscriber 
{
    // Subscriber constants
    private static final long serialVersionUID = 1L;
    private final Random random = new Random();
    private final String id;
    private final int crashAfter = 3;

    // Message parameters
    private int messageCount = 0;

    protected SubscriberImpl(String id, Channel channel) throws RemoteException 
    {
        super();
        this.id = id;
    }
    
    /* Default receive function */
    @Override
    public boolean receive(Message msg) throws RemoteException 
    {
        System.out.println(id + " received message: " + msg.getContent());
        return true;
    }

    /* Testing receive function, to simulate random delays */
    @Override
    public boolean receiveDelayed(Message msg) throws RemoteException 
    {
        if (id.equals("S1-C1")) /* Don't delay S1 - Testing */
        {
            System.out.println("[" + id + "] Processed message: " + msg.getMessageID());
            return true;  
        }

        int outcome = random.nextInt(100);
        if (outcome < 70) /* 70% chance to have no delay */
        {
            System.out.println("[" + id + "] Processed message: " + msg.getMessageID());
        }
        else if (outcome < 85) 
        {
            try 
            {
                Thread.sleep(300); /* 15% chance for short delay (300ms) */
            } 
            catch (InterruptedException e) 
            {
                Thread.currentThread().interrupt();
            }
            System.out.println("[" + id + "] Processed message after short delay: " + msg.getMessageID());
        } 
        else
        {
            try 
            {
                Thread.sleep(3000); /* 15% chance for long delay (3000ms) */
            } 
            catch (InterruptedException e) 
            {
                Thread.currentThread().interrupt();
            }
            System.out.println("[" + id + "] Processed message after long delay: " + msg.getMessageID());
        }
        return true;
    }
    
    /* Testing receive function, to simulate a crashing customer */
    @Override
    public boolean receiveCrashed(Message msg) throws RemoteException
    {
        if (id.equals("S1-C1"))  /* Don't crash S1 - Testing */
        {
            System.out.println("[" + id + "] Processed message: " + msg.getMessageID());
            return true;  
        }
        
        messageCount++;
        if (messageCount > crashAfter) 
        {
            System.out.println("[" + id + "] Simulating crash on message: " + msg.getMessageID());
            throw new RemoteException("Simulated crash: client is down");
        } 
        else 
        {
            System.out.println("[" + id + "] Processed message: " + msg.getMessageID());
            return true;
        }
    }

    @Override
    public String getID() 
    {
        return this.toString();
    }
    
    @Override
    public String toString() 
    {
        return "Subscriber[" + id + "]";
    }


}
