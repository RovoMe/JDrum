package at.rovo.caching.drum;

import at.rovo.caching.drum.event.DrumEvent;
import at.rovo.caching.drum.internal.backend.berkeley.BerkeleyDBStoreMergerFactory;
import at.rovo.caching.drum.internal.backend.dataStore.DataStoreMergerFactory;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class Test implements DrumListener, Dispatcher<String, String>
{
    /** The number of updates or answers to process before quitting the test **/
    private static final int N = 200000;
    private static final int CHUNK_SIZE = 10000;

    /** The DRUM instance used for testing **/
    private final Drum<String, String> drum;
    /** The timestamp the test was started at **/
    private final long start;
    /** The number of updates performed **/
    // private volatile int updates;
    private AtomicInteger updates = new AtomicInteger(0);
    /** The number of answers received **/
    // private volatile int answers;
    private AtomicInteger answers = new AtomicInteger(0);

    /** The lock object to synchronize **/
    private final Object statLock = new Object();
    /** A random number calculator **/
    private final Random random = new Random();

    public static void main(String[] args) throws Exception
    {
        try
        {
            Test main = new Test();
            new Thread(() ->
                       {
                           while (main.isRunning())
                           {
                               try
                               {
                                   Thread.sleep(1000);
                               }
                               catch (InterruptedException e)
                               {
                                   e.printStackTrace();
                                   break;
                               }
                               main.logStat();
                           }
                           System.out.println("FINISHED!");
                           try
                           {
                               main.dispose();
                           }
                           catch (DrumException e)
                           {
                               e.printStackTrace();
                           }
                       }).start();
            main.run();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public Test() throws Exception
    {
        int bufSize = 1 << 10;
        int buckets = 8;
        System.out.println("buckets " + buckets + ", buf " + bufSize);
        drum = new DrumBuilder<>("keyValueName", String.class, String.class).numBucket(buckets).bufferSize(bufSize)
                .dispatcher(this).listener(this)
                //.factory(DataStoreMergerFactory.class)
                .factory(BerkeleyDBStoreMergerFactory.class)
                .build();
        start = System.currentTimeMillis();
    }

    private void logStat()
    {
        long time = System.currentTimeMillis() - start;
        System.out.println(" updates " + updates.get() + " answers " + answers.get() + ", " + time / 1000 + " s, answers/s avg " +
                           (1000L * answers.get() / time));
    }

    private boolean isRunning()
    {
        return updates.get() < N || answers.get() < N;
    }

    private void run()
    {
        try
        {
            for (int i = 0; i < N / CHUNK_SIZE; i++)
            {
                for (int j = 0; j < CHUNK_SIZE; j++)
                {
                    long val = random.nextLong();
                    drum.checkUpdate(val, null, "" + val);
                    updates.incrementAndGet();
                }

                // synchronized (statLock) {
                // while (answers < updates) {
                // statLock.wait(1000);
                // }
                // }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        System.out.println("update finished");
    }

    private void dispose() throws DrumException
    {
        drum.dispose();
    }

    @Override
    public void update(DrumEvent<? extends DrumEvent<?>> event)
    {
    }

    @Override
    public void uniqueKeyCheck(Long key, String aux)
    {
        if (Long.parseLong(aux) != key)
        {
            System.out.println("False unique " + key + " " + aux);
        }
    }

    @Override
    public void duplicateKeyCheck(Long key, String value, String aux)
    {
        System.out.println("dup check " + key + " " + aux);
    }

    @Override
    public void uniqueKeyUpdate(Long key, String value, String aux)
    {
//        synchronized (statLock)
        {
            answers.incrementAndGet();
//            statLock.notifyAll();
        }
    }

    @Override
    public void duplicateKeyUpdate(Long key, String value, String aux)
    {
        System.out.println("dup update " + key + " " + aux);
//        synchronized (statLock)
        {
            answers.incrementAndGet();
//            statLock.notifyAll();
        }
    }

    @Override
    public void update(Long key, String value, String aux)
    {
    }
}
