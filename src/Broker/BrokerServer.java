package Broker;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerServer 
{
    // Broker Communication Constants
    private static final String BROKER_URL = "rmi://localhost/MessageBroker";
    private static final int PORT = 1099;
    public static void main(String[] args) 
    {
        try 
        {
            /* Start RMI registry */
            LocateRegistry.createRegistry(PORT);
            Broker broker = new BrokerImpl();
            System.out.println("RMI Registry created");

            /* Create channels */
            broker.createChannel("C1");
            broker.createChannel("C2");

            Naming.rebind(BROKER_URL, broker);
            System.out.println("RMI Registry ready");

            /* Check for new channels every minute */
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> 
            {
                try 
                {
                    List<String> channels = broker.listChannels();
                    System.out.println("Current Channels: " + channels);
                } 
                catch (RemoteException e) 
                {
                    System.err.println("Error retrieving channels: " + e.getMessage());
                    e.printStackTrace();
                }
            }, 60, 60, TimeUnit.SECONDS);

        } 
        catch (RemoteException re) 
        {
            System.err.println("RemoteException: " + re.getMessage());
            re.printStackTrace();
        } 
        catch (Exception e) 
        {
            System.err.println("BrokerServer exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
