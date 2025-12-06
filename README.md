# Message Middleware - Pub/Sub System

A Publish-Subscribe Message-Oriented Middleware system built with Java RMI, implementing a competing-consumers pattern (load balancing) rather than broadcasting. Provides robust message delivery with FIFO ordering within partitions and at-least-once delivery guarantees.

## Quick Start

### Prerequisites
- Java 8 or higher

### Running the System

**Terminal 1 - Broker Server:**
```bash
./compile.sh
```

**Terminal 2 - Subscribers:**
```bash
./run_subscriber.sh
```

**Terminal 3 - Publishers:**
```bash
./run_publisher.sh
```

### Basic Usage

```java
// Create a channel
Channel channel = /* get from RMI registry */;

// Subscribe
Subscriber subscriber = new SubscriberImpl("sub-1");
channel.subscribe(subscriber);

// Publish messages
Message msg = new Message("publisher-1", "channel-1", "Hello", partitionId, messageIndex);
channel.publish(msg);

// Get metrics
String metrics = channel.getMetrics();
System.out.println(metrics);

// Graceful shutdown
channel.shutdown();
```

---

## Architecture

### Queue Flow

1. **Global Queue** - All published messages enter here
2. **Partition Queues** - Messages distributed by partition ID (publisher ID + partition number)
3. **Subscriber Queues** - Complete partitions assigned to individual subscribers
4. **Backup Queue** - Maintains copies until delivery confirmation

### Thread Model

- **GlobalOffloader Thread** - Moves messages from global queue to partition queues
- **PartitionOffloader Thread** - Assigns partitions to subscribers with load balancing
- **Delivery Threads** - One per subscriber for message delivery with retry logic

---

## Configuration

All configuration is centralised in [ChannelConfig.java](src/Channel/ChannelConfig.java):

### Key Parameters

```java
// Queue Configuration
QUEUE_CAPACITY = 1000
MAX_DELIVERED_MESSAGES = 10000

// Delivery Configuration
MAX_DELIVERY_ATTEMPTS = 3
RETRY_DELAY_MS = 500

// Circuit Breaker Configuration
CIRCUIT_FAILURE_THRESHOLD = 5
CIRCUIT_TIMEOUT_MS = 30000
CIRCUIT_HALF_OPEN_TIMEOUT_MS = 10000

// Performance Tuning
BATCH_SIZE = 10
PARTITION_WEIGHT = 10
```

### Tuning Recommendations

**High Throughput:**
```java
BATCH_SIZE = 50
QUEUE_CAPACITY = 5000
```

**Low Latency:**
```java
BATCH_SIZE = 1
SUBSCRIBER_LOCK_TIMEOUT_MS = 10
```

**Unreliable Networks:**
```java
MAX_DELIVERY_ATTEMPTS = 5
RETRY_DELAY_MS = 1000
CIRCUIT_FAILURE_THRESHOLD = 10
```

---

## Metrics & Monitoring

Access metrics via `channel.getMetrics()`:

### Tracked Metrics
- Messages Published/Delivered/Failed/Dropped
- Duplicates Rejected
- Current Global Queue Size
- Active Partitions/Subscribers Count
- Circuits Open/Half-Open
- Restorations Performed/Messages Restored
- Average Delivery Time (ms)

### Example Output

```
========== Channel Metrics ==========
Messages Published:      1000
Messages Delivered:      998
Messages Failed:         2
Messages Dropped:        0
Duplicates Rejected:     0
Current Global Queue:    0
Active Partitions:       5
Active Subscribers:      2
Circuits Open:           0
Circuits Half-Open:      0
Restorations Performed:  0
Messages Restored:       0
Avg Delivery Time:       12.34 ms
=====================================
```

### Performance Benchmarks

- Single Subscriber: ~10,000 msg/sec
- Multiple Subscribers: Scales linearly
- Average Delivery Time: 10-15ms (local RMI)
- Circuit Breaker Overhead: <1ms
