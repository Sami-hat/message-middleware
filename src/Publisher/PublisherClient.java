package Publisher;
import java.rmi.Naming;

import Broker.Broker;
import Channel.Channel;
import Message.Message;

@SuppressWarnings("unused")
public class PublisherClient 
{
    // Broker Communication Constants
    private static final String BROKER_URL = "rmi://localhost/MessageBroker";
    private static final String EVENT1 = "C1";
    private static final String EVENT2 = "C2";

    // Message Constants
    private static final int MESSAGE_COUNT = 10;
    private static final int MESSAGE_DELAY_MS = 500;
    private static final String MESSAGE_PREFIX = "Test message ";

    public static void main(String[] args) 
    {
        try 
        {
            /* Lookup the broker from the RMI registry */
            Broker broker = (Broker) Naming.lookup(BROKER_URL);

            Publisher pub1 = new PublisherImpl("P1");

            /* Simulate publishing messages */
            for (int i = 1; i <= MESSAGE_COUNT; i++) 
            {
                /* Message requires: Source, Event Type (Channel), Contents, Partition, Index */
                Message msg = new Message("P1", EVENT1, MESSAGE_PREFIX + i, i%2, i);
                System.out.println("Publishing: " + msg);

                /* Get or create channel */
                Channel channel = broker.getChannel(msg.getType());
                if (channel == null) 
                {
                    System.out.println("Creating channel");
                    broker.createChannel(msg.getType());
                }
                channel.publish(msg);

                // if (i == MESSAGE_COUNT/2) channel.killQueues(); /* Used for crashing queues - Testing */
            }

            /* Simulate publishing messages to second channel */
            for (int i = 1; i <= MESSAGE_COUNT; i++) 
            {
                /* Message requires: Source, Event Type (Channel), Contents, Partition, Index */
                Message msg = new Message("P1", EVENT2, MESSAGE_PREFIX + i, i%2, i);
                System.out.println("Publishing: " + msg);

                /* Get or create channel */
                Channel channel = broker.getChannel(msg.getType());
                if (channel == null) 
                {
                    System.out.println("Creating channel");
                    broker.createChannel(msg.getType());
                }
                channel.publish(msg);
            }

            System.exit(0); /* Close publisher */

        } 
        catch (Exception e) 
        {
            System.err.println("Publisher error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
