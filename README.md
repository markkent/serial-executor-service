serial-executor-service
=======================

Test utility implementation of ScheduledExecutorService

Allows for testing of code such as the following:

class Foo
{
    private int count = 0;

    public Foo(ScheduledExecutorService service, Bar bar)
    {
        service.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run()
                {
                    count++;
                    bar.doWhatever();
                }
            }, 5, 10, TimeUnit.SECONDS);
    }

    public int getCount()
    {
        return count;
    }
}

Without the capabilities of this utility, testing the above would require doing something
like the following:

@Test
public void testFoo()
{
    Bar bar = new Bar();
    Foo foo = new Foo(Executors.newSingleThreadExecutor(), bar);

    Thread.sleep(5*1000);

    assertEquals(foo.getCount(), 1);
    assertTrue(bar.whateverGotDone());
}


Using this test class, the sleep is no longer required:

@Test
public void testFoo()
{
    SerialScheduledExecutorService executor = new SerialScheduledExecutorService();
    Bar bar = new Bar();
    Foo foo = new Foo(executor, bar);

    executor.elapseTime(5, TimeUnit.SECONDS);

    assertEquals(foo.getCount(), 1);
    assertTrue(bar.whateverGotDone());
}

For more examples, see the unit tests provided.
