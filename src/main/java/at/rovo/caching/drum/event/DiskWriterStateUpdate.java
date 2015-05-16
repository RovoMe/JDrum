package at.rovo.caching.drum.event;

public class DiskWriterStateUpdate extends DrumEvent<DiskWriterStateUpdate>
{
    private int bucketId = 0;
    private DiskWriterState state = null;


    public DiskWriterStateUpdate(String drumName, int bucketId, DiskWriterState state)
    {
        super(drumName, DiskWriterStateUpdate.class);
        this.bucketId = bucketId;
        this.state = state;
    }

    public int getBucketId()
    {
        return this.bucketId;
    }

    public DiskWriterState getState()
    {
        return this.state;
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(this.drumName);
        buffer.append(" - ");
        buffer.append(this.currentThread.getName());
        buffer.append(" - DiskWriter ");
        buffer.append(this.bucketId);
        buffer.append(" state changed to: ");
        buffer.append(this.state);

        return buffer.toString();
    }
}
