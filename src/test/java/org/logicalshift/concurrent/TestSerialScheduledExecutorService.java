package org.logicalshift.concurrent;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSerialScheduledExecutorService
{
    private SerialScheduledExecutorService executorService;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        executorService = new SerialScheduledExecutorService();
    }

    @Test
    public void testRunOnce()
            throws Exception
    {
        Counter counter = new Counter();
        executorService.execute(counter);

        assertEquals(counter.getCount(), 1);
    }

    @Test
    public void testThrownExceptionsAreSwallowedForRunOnceRunnable()
            throws Exception
    {
        executorService.execute(new Runnable()
        {
            @Override
            public void run()
            {
                throw new RuntimeException("deliberate");
            }
        });
    }

    @Test
    public void testSubmitRunnable()
            throws Exception
    {
        Counter counter = new Counter();
        Future<Integer> future = executorService.submit(counter, 10);

        assertEquals(counter.getCount(), 1);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals((int) future.get(), 10);
    }

    @Test
    public void testThrownExceptionsArePushedIntoFutureForSubmittedRunnable()
            throws Exception
    {
        Future<Integer> future = executorService.submit(new Runnable() {
            @Override
            public void run()
            {
                throw new RuntimeException("deliberate");
            }
        }, 10);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        try {
            future.get();
        }
        catch (Exception expected)
        {
            assertEquals(expected.getMessage(), "java.lang.RuntimeException: deliberate");
            return;
        }

        fail("Should have received exception");
    }

    @Test
    public void testSubmitCallable()
            throws Exception
    {
        CallableCounter counter = new CallableCounter();
        Future<Integer> future = executorService.submit(counter);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals((int) future.get(), 1);
    }

    @Test
    public void testThrownExceptionsArePushedIntoFutureForSubmittedCallable()
            throws Exception
    {
        Future<Integer> future = executorService.submit(new Callable<Integer>() {
            @Override
            public Integer call()
                    throws Exception
            {
                throw new Exception("deliberate");
            }
        });

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        try {
            future.get();
        }
        catch (Exception expected)
        {
            assertEquals(expected.getMessage(), "java.lang.Exception: deliberate");
            return;
        }

        fail("Should have received exception");
    }

    @Test
    public void testScheduleRunnable()
            throws Exception
    {
        Counter counter = new Counter();
        Future<?> future = executorService.schedule(counter, 10, TimeUnit.MINUTES);

        executorService.elapseTime(9, TimeUnit.MINUTES);

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 0);

        executorService.elapseTime(1, TimeUnit.MINUTES);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 1);
    }

    @Test
    public void testThrownExceptionsArePushedIntoFutureForScheduledRunnable()
            throws Exception
    {
        Future<?> future = executorService.schedule(new Runnable() {
            @Override
            public void run()
            {
                throw new RuntimeException("deliberate");
            }
        }, 10, TimeUnit.MINUTES);

        executorService.elapseTime(10, TimeUnit.MINUTES);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        boolean caught = false;
        try {
            future.get();
        }
        catch (Exception expected)
        {
            assertEquals(expected.getMessage(), "java.lang.RuntimeException: deliberate");
            caught = true;
        }

        assertTrue(caught, "Should have received exception");
    }

    @Test
    public void testScheduledRunnableWithZeroDelayCompletesImmediately()
            throws Exception
    {
        Counter counter = new Counter();
        Future<?> future = executorService.schedule(counter, 0, TimeUnit.MINUTES);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 1);
    }

    @Test
    public void testCancelScheduledRunnable()
            throws Exception
    {
        Counter counter = new Counter();
        Future<?> future = executorService.schedule(counter, 10, TimeUnit.MINUTES);

        executorService.elapseTime(9, TimeUnit.MINUTES);

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 0);

        future.cancel(true);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());

        executorService.elapseTime(1, TimeUnit.MINUTES);
        assertEquals(counter.getCount(), 0);
    }

    @Test
    public void testScheduleCallable()
            throws Exception
    {
        CallableCounter counter = new CallableCounter();
        Future<Integer> future = executorService.schedule(counter, 10, TimeUnit.MINUTES);

        executorService.elapseTime(9, TimeUnit.MINUTES);

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 0);

        executorService.elapseTime(1, TimeUnit.MINUTES);
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 1);
        assertEquals((int)future.get(), 1);
    }

    @Test
    public void testThrownExceptionsArePushedIntoFutureForScheduledCallable()
            throws Exception
    {
        Future<Integer> future = executorService.schedule(new Callable<Integer>()
        {
            @Override
            public Integer call()
                    throws Exception
            {
                throw new Exception("deliberate");
            }
        }, 10, TimeUnit.MINUTES);

        executorService.elapseTime(10, TimeUnit.MINUTES);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        boolean caught = false;
        try {
            future.get();
        }
        catch (Exception expected)
        {
            assertEquals(expected.getMessage(), "java.lang.Exception: deliberate");
            caught = true;
        }

        assertTrue(caught, "Should have received exception");
    }

    @Test
    public void testScheduledCallableWithZeroDelayCompletesImmediately()
            throws Exception
    {
        CallableCounter counter = new CallableCounter();
        Future<Integer> future = executorService.schedule(counter, 0, TimeUnit.MINUTES);

        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 1);
    }

    @Test(expectedExceptions = CancellationException.class)
    public void testCancelScheduledCallable()
            throws Exception
    {
        CallableCounter counter = new CallableCounter();
        Future<Integer> future = executorService.schedule(counter, 10, TimeUnit.MINUTES);

        executorService.elapseTime(9, TimeUnit.MINUTES);

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 0);

        future.cancel(true);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());

        executorService.elapseTime(1, TimeUnit.MINUTES);
        assertEquals(counter.getCount(), 0);

        // Should throw
        future.get();
    }

    @Test
    public void testRepeatingRunnable()
            throws Exception
    {
        Counter counter = new Counter();
        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(counter, 10, 5, TimeUnit.MINUTES);

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 0);

        // After 9 minutes, we shouldn't have run yet, and should have 1 minute left
        executorService.elapseTime(9, TimeUnit.MINUTES);
        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(future.getDelay(TimeUnit.MINUTES), 1);
        assertEquals(counter.getCount(), 0);

        // After 1 more minute, we should have run once, and should have 5 minutes remaining
        executorService.elapseTime(1, TimeUnit.MINUTES);
        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(future.getDelay(TimeUnit.MINUTES), 5);
        assertEquals(counter.getCount(), 1);

        // After another 10 minutes, we should have run twice more
        executorService.elapseTime(10, TimeUnit.MINUTES);
        assertEquals(counter.getCount(), 3);

    }

    @Test
    public void testRepeatingRunnableThatThrowsDoesNotRunAgain()
            throws Exception
    {
        FailingCounter counter = new FailingCounter(1);
        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(counter, 10, 5, TimeUnit.MINUTES);

        executorService.elapseTime(10, TimeUnit.MINUTES);
        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 1);

        // The runnable will throw on the second attempt
        executorService.elapseTime(5, TimeUnit.MINUTES);
        assertEquals(counter.getCount(), 2);
        assertTrue(future.isDone());
        boolean caught = false;
        try {
            future.get();
        }
        catch (Exception expected)
        {
            assertEquals(expected.getMessage(), "java.lang.RuntimeException: deliberate");
            caught = true;
        }

        assertTrue(caught, "Should have received exception");

        // The runnable should not execute again
        executorService.elapseTime(20, TimeUnit.MINUTES);
        assertEquals(counter.getCount(), 2);
    }

    @Test
    public void testRepeatingRunnableThatThrowsDoesNotRunAgainWhenElapseContainsMultipleInvocations()
            throws Exception
    {
        FailingCounter counter = new FailingCounter(1);
        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(counter, 10, 5, TimeUnit.MINUTES);

        executorService.elapseTime(10, TimeUnit.MINUTES);
        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 1);

        // The runnable will throw on the second attempt (out of three)
        executorService.elapseTime(10, TimeUnit.MINUTES);
        assertEquals(counter.getCount(), 2);
        assertTrue(future.isDone());
        boolean caught = false;
        try {
            future.get();
        }
        catch (Exception expected)
        {
            assertEquals(expected.getMessage(), "java.lang.RuntimeException: deliberate");
            caught = true;
        }

        assertTrue(caught, "Should have received exception");

        // The runnable should not execute again
        executorService.elapseTime(20, TimeUnit.MINUTES);
        assertEquals(counter.getCount(), 2);
    }

    @Test
    public void testRepeatingRunnableWithZeroDelayExecutesImmediately()
            throws Exception
    {
        Counter counter = new Counter();
        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(counter, 0, 5, TimeUnit.MINUTES);

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 1);
        assertEquals(future.getDelay(TimeUnit.MINUTES), 5);

        // After another 10 minutes, we should have run twice more
        executorService.elapseTime(10, TimeUnit.MINUTES);
        assertEquals(counter.getCount(), 3);
    }

    @Test
    public void testCancelRepeatingRunnableBeforeFirstRun()
            throws Exception
    {
        Counter counter = new Counter();
        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(counter, 10, 5, TimeUnit.MINUTES);

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 0);

        executorService.elapseTime(9, TimeUnit.MINUTES);

        future.cancel(true);

        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        assertEquals(counter.getCount(), 0);

        executorService.elapseTime(1, TimeUnit.MINUTES);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        assertEquals(counter.getCount(), 0);
    }

    @Test
    public void testCancelRepeatingRunnableAfterFirstRun()
            throws Exception
    {
        Counter counter = new Counter();
        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(counter, 10, 5, TimeUnit.MINUTES);

        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertEquals(counter.getCount(), 0);

        executorService.elapseTime(10, TimeUnit.MINUTES);

        future.cancel(true);

        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        assertEquals(counter.getCount(), 1);

        executorService.elapseTime(5, TimeUnit.MINUTES);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        assertEquals(counter.getCount(), 1);
    }

    @Test
    public void testMultipleRepeatingRunnables()
            throws Exception
    {
        Counter countEveryMinute = new Counter();
        Counter countEveryTwoMinutes = new Counter();
        ScheduledFuture<?> futureEveryMinute = executorService.scheduleAtFixedRate(countEveryMinute, 1, 1, TimeUnit.MINUTES);
        ScheduledFuture<?> futureEveryTwoMinutes = executorService.scheduleAtFixedRate(countEveryTwoMinutes, 2, 2, TimeUnit.MINUTES);

        executorService.elapseTime(7, TimeUnit.MINUTES);

        assertEquals(countEveryMinute.getCount(), 7);
        assertEquals(countEveryTwoMinutes.getCount(), 3);

        futureEveryMinute.cancel(true);

        executorService.elapseTime(1, TimeUnit.MINUTES);
        assertEquals(countEveryMinute.getCount(), 7);
        assertEquals(countEveryTwoMinutes.getCount(), 4);
    }

    static class Counter
            implements Runnable
    {
        private int count = 0;

        @Override
        public void run()
        {
            count++;
        }

        public int getCount()
        {
            return count;
        }
    }

    static class CallableCounter
            implements Callable<Integer>
    {
        private int count = 0;

        @Override
        public Integer call()
                throws Exception
        {
            return ++count;
        }

        public int getCount()
        {
            return count;
        }
    }

    static class FailingCounter
        implements Runnable
    {
        private int count = 0;
        private final int limit;

        FailingCounter(int limit)
        {
            this.limit = limit;
        }

        public int getCount()
        {
            return count;
        }

        @Override
        public void run()
        {
            count++;

            if (count > limit) {
                throw new RuntimeException("deliberate");
            }
        }
    }
}
