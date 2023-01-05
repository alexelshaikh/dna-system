package utils;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PooledCompletionService<T> extends ExecutorCompletionService<T> implements Streamable<T> {
    private long totalSubmittedJobs;
    private long currentlyAvailablePolls;
    private final ExecutorService pool;
    private final Lock lock;
    public PooledCompletionService(ExecutorService pool) {
        super(pool);
        this.pool = pool;
        this.totalSubmittedJobs = 0L;
        this.currentlyAvailablePolls = 0L;
        this.lock = new ReentrantLock();
    }

    @Override
    public Future<T> submit(Callable<T> task) {
        lock.lock();
        totalSubmittedJobs++;
        currentlyAvailablePolls++;
        lock.unlock();
        return super.submit(task);
    }

    @Override
    public Future<T> submit(Runnable task, T result) {
        lock.lock();
        totalSubmittedJobs++;
        currentlyAvailablePolls++;
        lock.unlock();
        return super.submit(task, result);
    }

    @Override
    public Future<T> take() {
        lock.lock();
        currentlyAvailablePolls--;
        lock.unlock();
        return FuncUtils.safeCall(super::take);
    }

    public T takeDirect() {
        return FuncUtils.safeCall(() -> take().get());
    }

    @Override
    public Future<T> poll() {
        lock.lock();
        Future<T> result = super.poll();
        if (result != null)
            currentlyAvailablePolls--;
        lock.unlock();
        return result;
    }

    @Override
    public Future<T> poll(long timeout, TimeUnit unit) {
        lock.lock();
        Future<T> result = FuncUtils.safeCall(() -> super.poll(timeout, unit));
        if (result != null)
            currentlyAvailablePolls--;
        lock.unlock();
        return result;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return isPollable();
            }

            @Override
            public T next() {
                return takeDirect();
            }
        };
    }

    public boolean isShutdown() {
        return pool.isShutdown();
    }

    public long getTotalSubmittedJobs() {
        return totalSubmittedJobs;
    }

    public long getCurrentlyAvailablePolls() {
        return currentlyAvailablePolls;
    }

    public void shutdown() {
        this.pool.shutdown();
    }

    public boolean isPollable() {
        return !pool.isShutdown() || currentlyAvailablePolls > 0L;
    }
}
