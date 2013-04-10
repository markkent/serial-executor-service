package org.logicalshift.concurrent;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Implements ScheduledExecutorService with a controllable time elapse. Tasks are run
 * in the context of the thread advancing time.
 * <p/>
 * Tasks are modelled as instantaneous events; tasks scheduled to be run at the same
 * instant will be run in the order of their registration.
 */
public class SerialScheduledExecutorService
        implements ScheduledExecutorService
{
    private final PriorityQueue<SerialScheduledFuture<?>> futureTasks = new PriorityQueue<SerialScheduledFuture<?>>();
    private Collection<SerialScheduledFuture<?>> tasks = futureTasks;
    private boolean isShutdown = false;

    @Override
    public void execute(Runnable runnable)
    {
        Preconditions.checkNotNull(runnable, "Task object is null");
        try {
            runnable.run();
        }
        catch (Throwable ignored) {
        }
    }

    @Override
    public void shutdown()
    {
        isShutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        shutdown();
        // Note: This doesn't seem to be quite right. It seems like I should return the unexecuted tasks that
        // were scheduled on the ScheduledExecutorService, but I don't know how to turn a Callable into a
        // Runnable (which is required since future tasks can be scheduled as Callables).
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown()
    {
        return isShutdown;
    }

    @Override
    public boolean isTerminated()
    {
        return isShutdown();
    }

    @Override
    public boolean awaitTermination(long l, TimeUnit timeUnit)
            throws InterruptedException
    {
        return true;
    }

    @Override
    public <T> Future<T> submit(Callable<T> tCallable)
    {
        Preconditions.checkNotNull(tCallable, "Task object is null");
        try {
            return Futures.immediateFuture(tCallable.call());
        }
        catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public <T> Future<T> submit(Runnable runnable, T t)
    {
        Preconditions.checkNotNull(runnable, "Task object is null");
        try {
            runnable.run();
            return Futures.immediateFuture(t);
        }
        catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public Future<?> submit(Runnable runnable)
    {
        Preconditions.checkNotNull(runnable, "Task object is null");
        return submit(runnable, null);
    }


    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> callables)
            throws InterruptedException
    {
        Preconditions.checkNotNull(callables, "Task object list is null");
        ImmutableList.Builder<Future<T>> resultBuilder = ImmutableList.builder();
        for (Callable<T> callable : callables) {
            try {
                resultBuilder.add(Futures.immediateFuture(callable.call()));
            }
            catch (Exception e) {
                resultBuilder.add(Futures.<T>immediateFailedFuture(e));
            }
        }
        return resultBuilder.build();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> callables, long l, TimeUnit timeUnit)
            throws InterruptedException
    {
        Preconditions.checkNotNull(callables, "Task object list is null");
        return invokeAll(callables);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> callables)
            throws InterruptedException, ExecutionException
    {
        Preconditions.checkNotNull(callables, "callables is null");
        Preconditions.checkArgument(!callables.isEmpty(), "callables is empty");
        try {
            return callables.iterator().next().call();
        }
        catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> callables, long l, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        return invokeAny(callables);
    }

    /**
     * Advance time by the given quantum.
     * <p/>
     * Scheduled tasks due for execution will be executed in the caller's thread.
     *
     * @param quantum the amount of time to advance
     * @param timeUnit the unit of the quantum amount
     */
    public void elapseTime(long quantum, TimeUnit timeUnit)
    {
        Preconditions.checkArgument(quantum > 0, "Time quantum must be a positive number");
        Preconditions.checkState(!isShutdown, "Trying to elapse time after shutdown");

        elapseTime(toMillis(quantum, timeUnit));
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable runnable, long l, TimeUnit timeUnit)
    {
        Preconditions.checkNotNull(runnable, "Task object is null");
        Preconditions.checkArgument(l >= 0, "Delay must not be negative");
        SerialScheduledFuture<?> future = new SerialScheduledFuture<Void>(new FutureTask<Void>(runnable, null), toMillis(l, timeUnit));
        if (l == 0) {
            future.task.run();
        }
        else {
            tasks.add(future);
        }
        return future;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> vCallable, long l, TimeUnit timeUnit)
    {
        Preconditions.checkNotNull(vCallable, "Task object is null");
        Preconditions.checkArgument(l >= 0, "Delay must not be negative");
        SerialScheduledFuture<V> future = new SerialScheduledFuture<V>(new FutureTask<V>(vCallable), toMillis(l, timeUnit));
        if (l == 0) {
            future.task.run();
        }
        else {
            tasks.add(future);
        }
        return future;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long l, long l1, TimeUnit timeUnit)
    {
        Preconditions.checkNotNull(runnable, "Task object is null");
        Preconditions.checkArgument(l >= 0, "Initial delay must not be negative");
        Preconditions.checkArgument(l1 > 0, "Repeating delay must be greater than 0");
        SerialScheduledFuture<?> future = new RecurringRunnableSerialScheduledFuture<Void>(runnable, null, toMillis(l, timeUnit), toMillis(l1, timeUnit));
        if (l == 0) {
            future.task.run();

            if (future.isFailed()) {
                return future;
            }

            future.restartDelayTimer();

        }
        tasks.add(future);
        return future;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, long l, long l1, TimeUnit timeUnit)
    {
        return scheduleAtFixedRate(runnable, l, l1, timeUnit);
    }

    class SerialScheduledFuture<T>
            implements ScheduledFuture<T>
    {
        long remainingDelayMillis;
        FutureTask<T> task;

        public SerialScheduledFuture(FutureTask<T> task, long delayMillis)
        {
            this.task = task;
            this.remainingDelayMillis = delayMillis;
        }

        public long remainingMillis()
        {
            return remainingDelayMillis;
        }

        // wind time off the clock, return the amount of used time in millis
        public long elapseTime(long quantumMillis)
        {
            if (task.isDone() || task.isCancelled()) {
                return 0;
            }

            if (remainingDelayMillis <= quantumMillis) {
                task.run();
                return remainingDelayMillis;
            }

            remainingDelayMillis -= quantumMillis;
            return quantumMillis;
        }

        public boolean isRecurring()
        {
            return false;
        }

        public void restartDelayTimer()
        {
            throw new UnsupportedOperationException("Can't restart a non-recurring task");
        }

        @Override
        public long getDelay(TimeUnit timeUnit)
        {
            return timeUnit.convert(remainingDelayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed delayed)
        {
            if (delayed instanceof SerialScheduledFuture) {
                SerialScheduledFuture other = (SerialScheduledFuture) delayed;
                return Longs.compare(this.remainingDelayMillis, other.remainingDelayMillis);
            }
            return Longs.compare(this.remainingMillis(), delayed.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean b)
        {
            return task.cancel(b);
        }

        @Override
        public boolean isCancelled()
        {
            return task.isCancelled();
        }

        @Override
        public boolean isDone()
        {
            return task.isDone() || task.isCancelled();
        }

        public boolean isFailed()
        {
            if (!isDone()) {
                return false;
            }

            try {
                task.get();
            }
            catch (Throwable ignored) {
                return true;
            }
            return false;
        }

        @Override
        public T get()
                throws InterruptedException, ExecutionException
        {
            if (isCancelled()) {
                throw new CancellationException();
            }

            if (!isDone()) {
                throw new IllegalStateException("Called get() before result was available in SerialScheduledFuture");
            }

            return task.get();
        }

        @Override
        public T get(long l, TimeUnit timeUnit)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            return get();
        }
    }

    class RecurringRunnableSerialScheduledFuture<T>
            extends SerialScheduledFuture<T>
    {
        private final long recurringDelayMillis;
        private final Runnable runnable;
        private final T value;

        RecurringRunnableSerialScheduledFuture(Runnable runnable, T value, long initialDelayMillis, long recurringDelayMillis)
        {
            super(new FutureTask<T>(runnable, value), initialDelayMillis);
            this.runnable = runnable;
            this.value = value;
            this.recurringDelayMillis = recurringDelayMillis;
        }

        @Override
        public boolean isRecurring()
        {
            return true;
        }

        @Override
        public void restartDelayTimer()
        {
            task = new FutureTask<T>(runnable, value);
            remainingDelayMillis = recurringDelayMillis;
        }
    }

    private void elapseTime(long quantum)
    {
        List<SerialScheduledFuture<?>> toRequeue = newArrayList();

        // Redirect where the external interface queues up tasks to a temporary
        // collection. This allows the scheduled tasks to schedule future tasks.
        Collection<SerialScheduledFuture<?>> originalTasks = tasks;
        tasks = newArrayList();
        try {
            SerialScheduledFuture<?> current;
            while ((current = futureTasks.poll()) != null) {
                if (current.isCancelled()) {
                    // Drop cancelled tasks
                    continue;
                }

                if (current.isDone()) {
                    throw new AssertionError("Found a done task in the queue (contrary to expectation)");
                }

                // Try to elapse the time quantum off the current item
                long used = current.elapseTime(quantum);

                // If the item isn't done yet, and didn't fail, we'll need to put it back in the queue
                if (!current.isDone()) {
                    toRequeue.add(current);
                    continue;
                }

                if (used < quantum) {
                    // Partially used the quantum. Elapse the used portion off the rest of the queue so that we can reinsert
                    // this item in its correct spot (if necessary) before continuing with the rest of the quantum. This is
                    // because tasks may execute more than once during a single call to elapse time. We do this recursively
                    // out of convenience. Because this task is the next one that needs to run, all other tasks will need to
                    // run no more than once. When done, any new tasks that were added by the tasks that ran can be added to
                    // the queue for processing.
                    elapseTime(used);
                    rescheduleTaskIfRequired(futureTasks, current);
                    futureTasks.addAll(tasks);
                    tasks.clear();
                    quantum -= used;
                }
                else {
                    // Completely used the quantum, once we're done with this pass through the queue, may want need to add it back
                    rescheduleTaskIfRequired(toRequeue, current);
                }
            }
        }
        finally {
            futureTasks.addAll(toRequeue);
            futureTasks.addAll(tasks);
            tasks.clear();
            tasks = originalTasks;
        }
    }

    private static void rescheduleTaskIfRequired(Collection<SerialScheduledFuture<?>> tasks, SerialScheduledFuture<?> task)
    {
        if (task.isRecurring() && !task.isFailed()) {
            task.restartDelayTimer();
            tasks.add(task);
        }
    }

    private static long toMillis(long quantum, TimeUnit timeUnit)
    {
        return TimeUnit.MILLISECONDS.convert(quantum, timeUnit);
    }
}
