package at.rovo.caching.drum.testUtils;

import at.rovo.caching.drum.data.AppendableData;
import at.rovo.caching.drum.util.DrumUtils;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

public class PLDTestData implements AppendableData<PLDTestData>, Comparable<PLDTestData>
{
    private long hash = 0;
    private int budget = 0;
    private Set<Long> indegreeNeighbors = null;
    private transient String pld = null;

    public PLDTestData()
    {
        this.indegreeNeighbors = new LinkedHashSet<>();
    }

    public PLDTestData(long hash, int budget, Set<Long> indegreeNeighbors)
    {
        this.hash = hash;
        this.budget = budget;
        this.indegreeNeighbors = indegreeNeighbors;
    }

    public long getHash()
    {
        return this.hash;
    }

    public int getIndegree()
    {
        return this.indegreeNeighbors.size();
    }

    public int getBudget()
    {
        return this.budget;
    }

    public Set<Long> getIndegreeNeighbors()
    {
        return this.indegreeNeighbors;
    }

    public String getPLD()
    {
        return this.pld;
    }

    public void setHash(long hash)
    {
        this.hash = hash;
    }

    public void setBudget(int budget)
    {
        this.budget = budget;
    }

    public void setIndegreeNeighbors(Set<Long> indegreeNeighbors)
    {
        this.indegreeNeighbors = indegreeNeighbors;
    }

    public void setPLD(String pld)
    {
        this.pld = pld;
    }

    public void addIndegreeNeighbors(Set<Long> indegreeNeighbors)
    {
        this.indegreeNeighbors.addAll(indegreeNeighbors);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof PLDTestData)
        {
            PLDTestData data = (PLDTestData) obj;
            if (data.getHash() == this.hash)
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = 17;
        result = 31 * result + new Long(this.hash).hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append("Hash: ");
        buffer.append(this.hash);
        buffer.append("; neighbors: {");
        int size = this.indegreeNeighbors.size();
        int i = 0;
        for (Long neighbor : this.indegreeNeighbors)
        {
            i++;
            buffer.append(neighbor);
            if (i != size)
            {
                buffer.append(", ");
            }
        }
        buffer.append("}");

        return buffer.toString();
    }

    @Override
    public void append(PLDTestData data)
    {
        if (this.indegreeNeighbors == null)
        {
            this.indegreeNeighbors = new TreeSet<>();
        }
        if (data != null)
        {
            this.indegreeNeighbors.addAll(data.getIndegreeNeighbors());
        }
    }

    public synchronized byte[] toBytes()
    {
        int size = 12 + 8 * this.indegreeNeighbors.size() + 4;
        byte[] totalBytes = new byte[size];
        byte[] keyBytes = DrumUtils.long2bytes(this.hash); // 8 bytes
        System.arraycopy(keyBytes, 0, totalBytes, 0, 8);
        byte[] neighborSize = DrumUtils.int2bytes(this.indegreeNeighbors.size());
        System.arraycopy(neighborSize, 0, totalBytes, 8, 4); // 4 bytes
        int pos = 12;
        for (Long neighbor : this.indegreeNeighbors)
        {
            byte[] neighborBytes = DrumUtils.long2bytes(neighbor);
            System.arraycopy(neighborBytes, 0, totalBytes, pos, 8);
            pos += 8;
        }
        byte[] budget = DrumUtils.int2bytes(this.budget);
        System.arraycopy(budget, 0, totalBytes, pos, 4); // 4 bytes

        return totalBytes;
    }

    public synchronized PLDTestData readBytes(byte[] bytes)
    {
        byte[] keyBytes = new byte[8];
        System.arraycopy(bytes, 0, keyBytes, 0, 8);
        long hash = DrumUtils.byte2long(keyBytes);

        byte[] valueSizeBytes = new byte[4];
        System.arraycopy(bytes, 8, valueSizeBytes, 0, 4);
        int valueSize = DrumUtils.bytes2int(valueSizeBytes);

        TreeSet<Long> indegreeNeighbors = new TreeSet<>();

        int pos = 12;
        for (int i = 0; i < valueSize; i++)
        {
            byte[] valueBytes = new byte[8];
            System.arraycopy(bytes, pos, valueBytes, 0, 8);
            indegreeNeighbors.add(DrumUtils.byte2long(valueBytes));
            pos += 8;
        }

        byte[] budgetBytes = new byte[4];
        System.arraycopy(bytes, pos, budgetBytes, 0, 4);
        int budget = DrumUtils.bytes2int(budgetBytes);

        PLDTestData data = new PLDTestData();
        data.setHash(hash);
        data.setIndegreeNeighbors(indegreeNeighbors);
        data.setBudget(budget);

        return data;
    }

    @Override
    public int compareTo(PLDTestData o)
    {
        if (this.getHash() < o.getHash())
        {
            return -1;
        }
        else if (this.getHash() > o.getHash())
        {
            return 1;
        }

        return 0;
    }
}
