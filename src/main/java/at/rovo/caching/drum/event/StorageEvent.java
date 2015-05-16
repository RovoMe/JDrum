package at.rovo.caching.drum.event;

public class StorageEvent extends DrumEvent<StorageEvent>
{
    private long numEntries = 0L;

    public StorageEvent(String drumName, long numEntries)
    {
        super(drumName, StorageEvent.class);
        this.numEntries = numEntries;
    }

    public long getNumberOfEntries()
    {
        return this.numEntries;
    }
}
