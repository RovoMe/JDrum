package at.rovo.drum.datastore.simple.utils;

import at.rovo.drum.data.AppendableData;
import at.rovo.drum.util.DrumUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

@SuppressWarnings("Duplicates")
public class PLDTestData implements AppendableData<PLDTestData>, Comparable<PLDTestData> {

    private long hash = 0;
    private int budget = 0;
    private Set<Long> indegreeNeighbors;
    private transient String pld = null;

    public PLDTestData() {
        this.indegreeNeighbors = new LinkedHashSet<>();
    }

    public PLDTestData(final long hash, final int budget, @Nonnull final Set<Long> indegreeNeighbors) {
        this.hash = hash;
        this.budget = budget;
        this.indegreeNeighbors = indegreeNeighbors;
    }

    public long getHash() {
        return this.hash;
    }

    public int getIndegree() {
        return this.indegreeNeighbors.size();
    }

    public int getBudget() {
        return this.budget;
    }

    @Nonnull
    public Set<Long> getIndegreeNeighbors() {
        return this.indegreeNeighbors;
    }

    @Nonnull
    public String getPLD() {
        return this.pld;
    }

    public void setHash(final long hash) {
        this.hash = hash;
    }

    public void setBudget(final int budget) {
        this.budget = budget;
    }

    public void setIndegreeNeighbors(@Nonnull final Set<Long> indegreeNeighbors) {
        this.indegreeNeighbors = indegreeNeighbors;
    }

    public void setPLD(@Nonnull final String pld) {
        this.pld = pld;
    }

    public void addIndegreeNeighbors(@Nonnull final Set<Long> indegreeNeighbors) {
        this.indegreeNeighbors.addAll(indegreeNeighbors);
    }

    @Override
    public boolean equals(@Nullable final Object obj) {
        if (obj == this) {
            return true;
        }
        if (null == obj) {
            return false;
        }
        if (obj instanceof PLDTestData) {
            PLDTestData data = (PLDTestData) obj;
            return data.getHash() == this.hash;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Long.valueOf(this.hash).hashCode();
        return result;
    }

    @Override
    @Nullable
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        buffer.append("Hash: ");
        buffer.append(this.hash);
        buffer.append("; neighbors: {");
        int size = this.indegreeNeighbors.size();
        int i = 0;
        for (Long neighbor : this.indegreeNeighbors) {
            i++;
            buffer.append(neighbor);
            if (i != size) {
                buffer.append(", ");
            }
        }
        buffer.append("}");

        return buffer.toString();
    }

    @Override
    public void append(@Nonnull final PLDTestData data) {
        if (this.indegreeNeighbors == null) {
            this.indegreeNeighbors = new TreeSet<>();
        }
        this.indegreeNeighbors.addAll(data.getIndegreeNeighbors());
    }

    private void writeObject(@Nonnull final ObjectOutputStream stream) throws IOException {
        // 8 bytes long - hash
        stream.writeLong(this.hash);
        // 4 bytes int - length of indegreeNeighobrs
        stream.writeInt(this.indegreeNeighbors.size());
        for (Long neighbor : this.indegreeNeighbors) {
            // 8 bytes long - neighbor hash
            stream.writeLong(neighbor);
        }
        // 4 bytes int - budget value
        stream.writeInt(this.budget);
    }

    private void readObject(@Nonnull final ObjectInputStream stream) throws IOException, ClassNotFoundException {
        this.indegreeNeighbors = new LinkedHashSet<>();

        // read hash
        this.hash = stream.readLong();
        // read neighbors length
        int neighborsLength = stream.readInt();
        for (int i = 0; i < neighborsLength; i++) {
            long neighborHash = stream.readLong();
            this.indegreeNeighbors.add(neighborHash);
        }
        this.budget = stream.readInt();
    }

    @Nonnull
    @Override
    public byte[] toBytes() {
        final int size = 12 + 8 * this.indegreeNeighbors.size() + 4;
        final byte[] totalBytes = new byte[size];
        final byte[] keyBytes = DrumUtils.long2bytes(this.hash); // 8 bytes
        System.arraycopy(keyBytes, 0, totalBytes, 0, 8);
        final byte[] neighborSize = DrumUtils.int2bytes(this.indegreeNeighbors.size());
        System.arraycopy(neighborSize, 0, totalBytes, 8, 4); // 4 bytes
        int pos = 12;
        for (Long neighbor : this.indegreeNeighbors) {
            byte[] neighborBytes = DrumUtils.long2bytes(neighbor);
            System.arraycopy(neighborBytes, 0, totalBytes, pos, 8);
            pos += 8;
        }
        final byte[] budget = DrumUtils.int2bytes(this.budget);
        System.arraycopy(budget, 0, totalBytes, pos, 4); // 4 bytes

        return totalBytes;
    }

    @Nonnull
    @Override
    public PLDTestData readBytes(@Nonnull final byte[] bytes) {
        final byte[] keyBytes = new byte[8];
        System.arraycopy(bytes, 0, keyBytes, 0, 8);
        final long hash = DrumUtils.byte2long(keyBytes);

        final byte[] valueSizeBytes = new byte[4];
        System.arraycopy(bytes, 8, valueSizeBytes, 0, 4);
        final int valueSize = DrumUtils.bytes2int(valueSizeBytes);

        final TreeSet<Long> indegreeNeighbors = new TreeSet<>();

        int pos = 12;
        for (int i = 0; i < valueSize; i++) {
            final byte[] valueBytes = new byte[8];
            System.arraycopy(bytes, pos, valueBytes, 0, 8);
            indegreeNeighbors.add(DrumUtils.byte2long(valueBytes));
            pos += 8;
        }

        final byte[] budgetBytes = new byte[4];
        System.arraycopy(bytes, pos, budgetBytes, 0, 4);
        final int budget = DrumUtils.bytes2int(budgetBytes);

        final PLDTestData data = new PLDTestData();
        data.setHash(hash);
        data.setIndegreeNeighbors(indegreeNeighbors);
        data.setBudget(budget);

        return data;
    }

    @Override
    public int compareTo(@Nonnull final PLDTestData o) {
        if (this.getHash() < o.getHash()) {
            return -1;
        } else if (this.getHash() > o.getHash()) {
            return 1;
        }

        return 0;
    }
}
