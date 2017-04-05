package at.rovo.caching.drum.util.lockfree;

import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.internal.InMemoryEntry;
import java.awt.GridLayout;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FlippableDataContainerTest
{
    private AtomicInteger[] itemsDone;
    private GUI gui;

    @Test
    public void test() throws Exception {
        final int numThreads =  50;
        final int numItems = 2000;
        final CyclicBarrier barrier = new CyclicBarrier(numThreads+1);
        ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        FlippableDataContainer<InMemoryEntry<Integer, String>> sut = new FlippableDataContainer<>();

        this.itemsDone = new AtomicInteger[numThreads];
        for (int i=0; i<numThreads; i++) {
            itemsDone[i] = new AtomicInteger();
            threadPool.execute(new Worker(i, sut, numItems, barrier, this));
        }

        // helper to visualize which threads work and which don't
        // gui = new GUI(numThreads, numItems);

        // all threads should be initialized and waiting for the barrier to kick off and let them do their job
        barrier.await();
        barrier.reset();

        // wait till all worker threads finished
        System.out.println("Released worker threads. Waiting on them to finish their duty");
        barrier.await();

        Queue<InMemoryEntry<Integer, String>> data = sut.flip();
        assertThat(data.size(), is(equalTo(numThreads * numItems)));
    }

//    private void notify(int worker) {
//        gui.update(worker, itemsDone[worker].incrementAndGet());
//    }

    private final class Worker implements Runnable {

        private final FlippableDataContainer<InMemoryEntry<Integer, String>> container;
        private final CyclicBarrier barrier;
        private final int numGenerated;
        private final Random random = new Random();
        private final int workerId;
        private final FlippableDataContainerTest host;

        Worker(int num, FlippableDataContainer<InMemoryEntry<Integer, String>> container,
               int numGenerated, CyclicBarrier barrier, FlippableDataContainerTest host) {
            this.container = container;
            this.barrier = barrier;
            this.numGenerated = numGenerated;
            this.workerId = num;
            this.host = host;
        }

        @Override
        public void run()
        {
            int i = numGenerated;
            final String name = "Worker-" + workerId;
            try {
                System.out.println(name + " ready and waiting for the start signal");
                barrier.await();
                System.out.println(name + " starting work");
                while (i > 0) {
                    int val = random.nextInt();
                    InMemoryEntry<Integer, String> data =
                            new InMemoryEntry<>(random.nextLong(), val, name, DrumOperation.UPDATE);
                    System.out.println(name + " attempting to add value " + val);
                    container.put(data);
                    i--;
//                    this.host.notify(workerId);
                }
            } catch (InterruptedException iex) {
                System.err.println(name + " got interrupted! " + i + " entries could not be added!");
                iex.printStackTrace();
            } catch (BrokenBarrierException bbex) {
                System.err.println(name + " noticed a broken barrier. " + i + " entries could not be added!");
                bbex.printStackTrace();
            } finally {
                try {
                    System.out.println(name + " finished. Waiting for synchronization");
                    barrier.await(30, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    System.err.println(name + " failed waiting for synchronization due to " + ex.getLocalizedMessage());
                    ex.printStackTrace();
                }
            }
        }
    }

    private final class GUI extends JFrame  {

        private JProgressBar[] workerProgress;

        GUI(int numWorker, int items) {
            JPanel panel = new JPanel();
            panel.setLayout(new GridLayout(numWorker, 2));
            add(panel);
            workerProgress = new JProgressBar[numWorker];
            for (int i=0; i<numWorker; i++) {
                panel.add(new JLabel("Worker-" + i));
                workerProgress[i] =  new JProgressBar(0, items);
                workerProgress[i].setValue(0);
                panel.add(workerProgress[i]);
            }

            this.pack();
            this.setVisible(true);
        }

        void update(int worker, int items) {
            workerProgress[worker].setValue(items);
            this.update(this.getGraphics());
        }
    }
}
