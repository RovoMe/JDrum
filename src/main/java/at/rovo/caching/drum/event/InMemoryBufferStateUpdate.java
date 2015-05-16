package at.rovo.caching.drum.event;

public class InMemoryBufferStateUpdate extends DrumEvent<InMemoryBufferStateUpdate>
{
    private InMemoryBufferState state = null;
    private int bucketId = 0;

    public InMemoryBufferStateUpdate(String drumName, int bucketId, InMemoryBufferState state)
    {
        super(drumName, InMemoryBufferStateUpdate.class);
        this.bucketId = bucketId;
        this.state = state;
    }

    public int getBucketId()
    {
        return this.bucketId;
    }

    public InMemoryBufferState getState()
    {
        return this.state;
    }

    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(this.drumName);
        buffer.append(" - ");
        buffer.append(this.currentThread.getName());
        buffer.append(" - InMemoryBuffer ");
        buffer.append(this.bucketId);
        buffer.append(" state changed to: ");
        buffer.append(this.state);

        return buffer.toString();
    }
}
