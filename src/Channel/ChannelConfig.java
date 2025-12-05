package Channel;

public class ChannelConfig
{
    /* Queue configuration */
    public static final int QUEUE_CAPACITY = 1000;
    public static final int MAX_DELIVERED_MESSAGES = 10000;

    /* Delivery configuration */
    public static final int MAX_DELIVERY_ATTEMPTS = 3;
    public static final int MAX_DELIVERY_DELAYS = 3;
    public static final long MAX_DELIVERY_DELAY_MS = 3000;
    public static final long MAX_DELIVERY_INTERRUPT_MS = 300;
    public static final long RETRY_DELAY_MS = 500;

    /* Circuit breaker configuration */
    public static final int CIRCUIT_FAILURE_THRESHOLD = 5;
    public static final long CIRCUIT_TIMEOUT_MS = 30000;
    public static final long CIRCUIT_HALF_OPEN_TIMEOUT_MS = 10000;

    /* Load balancing configuration */
    public static final int PARTITION_WEIGHT = 10;

    /* Thread configuration */
    public static final long SUBSCRIBER_LOCK_TIMEOUT_MS = 1000;

    /* Batch processing configuration */
    public static final int BATCH_SIZE = 10;
    public static final long BATCH_TIMEOUT_MS = 100;

    /* Prevent instantiation */
    private ChannelConfig() {}
}
