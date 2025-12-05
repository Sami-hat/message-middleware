package Message;
import java.io.Serializable;
import java.time.LocalDateTime;

public class Message implements Serializable 
{
    // Message Parameters
    private static final long serialVersionUID = 1L;
    private final String content;
    private final LocalDateTime timestamp;
    private final String source;
    private final String type;
    private final int partition; 
    private final int index; 

    public Message(String source, String type, String content, int partition, int index) 
    {
        this.source = source; /* The publisher */
        this.type = type; /* The channel name it is going to */
        this.content = content; /* Contents */
        this.partition = partition; /* Partition number */
        this.index = index; /* Index in partition */
        this.timestamp = LocalDateTime.now(); /* Timestamp */
    }
    
    /* Getters */
    public String getSource() 
    {
        return source;
    }

    public String getType() 
    {
        return type;
        
    }

    public String getContent() 
    {
        return content;
    }

    public LocalDateTime getTimestamp() 
    {
        return timestamp;
    }

    public int getPartition() 
    {
        return partition;
    }

    public int getIndex() 
    {
        return index;
    }

    public String getPartitionID() 
    {
        return source + "-" + partition;
    }

    public String getMessageID() 
    {
        return source + "-" + partition + "-" + index;
    }

    /* Print */
    @Override
    public String toString() 
    {
        return "[" + timestamp + "] " + content;
    }
}
