package at.rovo.drum.util.lockfree;

import at.rovo.drum.DrumOperation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@DisplayName("FlippingDataContainer")
class FlippingDataContainerTest {

    @Test
    @DisplayName("Should add item into empty container")
    void shouldAddItemIntoEmptyContainer() {
        FlippingDataContainer<TestMemoryEntry<Integer, String>> sut = new FlippingDataContainer<>();

        sut.put(new TestMemoryEntry<>(1L, 1, "Test", DrumOperation.CHECK));

        assertThat(sut.isEmpty(), is(equalTo(false)));
    }

    @Test
    @DisplayName("Should not add null values")
    void shouldIgnoreNullValue() {
        FlippingDataContainer<TestMemoryEntry<Integer, String>> sut = new FlippingDataContainer<>();

        sut.put(null);

        assertThat(sut.isEmpty(), is(equalTo(true)));
    }

    @Test
    @DisplayName("Should contain empty container after flip")
    void shouldContainEmptyContainerAfterFlip() {
        FlippingDataContainer<TestMemoryEntry<Integer, String>> sut = new FlippingDataContainer<>();
        sut.put(new TestMemoryEntry<>(1L, 1, "Test", DrumOperation.CHECK));

        sut.flip();

        assertThat(sut.isEmpty(), is(equalTo(true)));
    }

    @Test
    @DisplayName("Should contain added items in flipped container")
    void shouldContainAddedItemsInFlippedContainer() {
        FlippingDataContainer<TestMemoryEntry<Integer, String>> sut = new FlippingDataContainer<>();
        sut.put(new TestMemoryEntry<>(1L, 1, "Test", DrumOperation.CHECK));
        sut.put(new TestMemoryEntry<>(2L, 2, "Test2", DrumOperation.APPEND_UPDATE));
        sut.put(new TestMemoryEntry<>(3L, 3, "Test3", DrumOperation.UPDATE));

        Queue<TestMemoryEntry<Integer, String>> queue = sut.flip();

        assertThat(queue.size(), is(equalTo(3)));
        assertThat(sut.isEmpty(), is(equalTo(true)));
    }

    @Test
    @DisplayName("Should store duplicate entries")
    void shouldStoreDuplicateEntries() {
        FlippingDataContainer<TestMemoryEntry<Integer, String>> sut = new FlippingDataContainer<>();
        sut.put(new TestMemoryEntry<>(1L, 1, "Test", DrumOperation.CHECK));
        sut.put(new TestMemoryEntry<>(1L, 1, "Test", DrumOperation.CHECK));

        Queue<TestMemoryEntry<Integer, String>> queue = sut.flip();

        assertThat(queue.size(), is(equalTo(2)));
        assertThat(sut.isEmpty(), is(equalTo(true)));
    }

    @Nested
    @DisplayName("When run concurrently")
    class ConcurrentTest {

        private final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        @Test
        @DisplayName("Should process all items correctly")
        void test() throws Exception {
            final int numWorkerThreads = 50;
            final int numItems = 20000;
            final CyclicBarrier barrier = new CyclicBarrier(numWorkerThreads + 2);
            ExecutorService threadPool = Executors.newFixedThreadPool(numWorkerThreads + 1);
            FlippingDataContainer<TestMemoryEntry<Integer, String>> sut = new FlippingDataContainer<>();

            Consumer consumer = new Consumer(sut, barrier);
            threadPool.execute(consumer);
            for (int i = 0; i < numWorkerThreads; i++) {
                threadPool.execute(new Worker(i, sut, numItems, barrier));
            }

            // all threads should be initialized and waiting for the barrier to kick off and let them do their job
            barrier.await();
            barrier.reset();

            // wait till all worker threads finished
            LOG.debug("Released worker threads. Waiting on them to finish their duty");
            barrier.await();

            shudwondAndAwaitTermination(threadPool);

            LOG.debug("Performing asserts");

            int itemsDone = consumer.getItemsDone();
            LOG.debug("Consumed " + itemsDone + " items so far");
            Queue<TestMemoryEntry<Integer, String>> queue = sut.flip();
            LOG.debug("Collected " + queue.size() + " further items");
            assertThat(sut.isEmpty(), is(equalTo(true)));
            itemsDone += queue.size();
            assertThat(itemsDone, is(equalTo(numWorkerThreads * numItems)));
        }

        private void shudwondAndAwaitTermination(ExecutorService threadPool) {
            threadPool.shutdown();
            try {
                if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    threadPool.shutdown();
                    if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                        LOG.warn("Could not terminate thread-pool due to pending thread");
                    }
                } else {
                    LOG.debug("Thread-Pool successfully stopped");
                }
            } catch (InterruptedException iEx) {
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        private final class Worker implements Runnable {

            private final FlippingDataContainer<TestMemoryEntry<Integer, String>> container;
            private final CyclicBarrier barrier;
            private final int numGenerated;
            private final Random random = new Random();
            private final int workerId;

            Worker(int num, FlippingDataContainer<TestMemoryEntry<Integer, String>> container,
                   int numGenerated, CyclicBarrier barrier) {
                this.container = container;
                this.barrier = barrier;
                this.numGenerated = numGenerated;
                this.workerId = num;
            }

            @Override
            public void run() {
                int i = numGenerated;
                final String name = "Worker-" + workerId;
                try {
                    barrier.await();
                    while (i > 0) {
                        int val = random.nextInt();
                        TestMemoryEntry<Integer, String> data =
                                new TestMemoryEntry<>(random.nextLong(), val, name, DrumOperation.UPDATE);
                        container.put(data);
                        i--;
                    }
                } catch (InterruptedException iex) {
                    LOG.warn(name + " got interrupted! " + i + " entries could not be added!");
                    Thread.currentThread().interrupt();
                } catch (BrokenBarrierException bbex) {
                    LOG.warn(name + " noticed a broken barrier. " + i + " entries could not be added!", bbex);
                } finally {
                    try {
                        LOG.debug(name + " finished. Waiting for synchronization");
                        barrier.await(30, TimeUnit.SECONDS);
                    } catch (Exception ex) {
                        LOG.warn(name + " failed waiting for synchronization due to " + ex.getLocalizedMessage(), ex);
                    }
                }
            }
        }

        private final class Consumer implements Runnable {

            private final FlippingDataContainer<TestMemoryEntry<Integer, String>> container;
            private final CyclicBarrier barrier;

            private int itemsDone = 0;

            Consumer(FlippingDataContainer<TestMemoryEntry<Integer, String>> container, CyclicBarrier barrier) {
                this.container = container;
                this.barrier = barrier;
            }

            @Override
            public void run() {
                try {
                    barrier.await();
                    int size;
                    do {
                        Thread.sleep(20);
                        size = container.flip().size();
                        itemsDone += size;
                        LOG.debug("Flipped " + size + " items");
                    } while (size > 0);
                } catch (InterruptedException iEx) {
                    LOG.warn("Consumer thread got interrupted");
                    Thread.currentThread().interrupt();
                } catch (BrokenBarrierException bbex) {
                    LOG.warn("Consumer thread noticed a broken barrier.", bbex);
                } finally {
                    try {
                        LOG.debug("Consumer thread finished. Waiting on synchronization");
                        barrier.await(30, TimeUnit.SECONDS);
                    } catch (Exception ex) {
                        LOG.warn("Consumer thread failed waiting for synchronization due to " + ex.getLocalizedMessage(), ex);
                    }
                }
            }

            int getItemsDone() {
                return this.itemsDone;
            }
        }
    }
}
