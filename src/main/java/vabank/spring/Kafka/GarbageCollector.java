package vabank.spring;

public class GarbageCollector extends Thread
{
    private long interval;

    public GarbageCollector(long interval)
    {
        this.interval=interval;
    }

    public void run()
    {
        Thread.currentThread().setPriority(Thread.MIN_PRIORITY);

        Runtime r=Runtime.getRuntime();
        long currentTotalMemory,minUsage=0;

        try
        {
            minUsage=r.totalMemory()-r.freeMemory();
        }
        catch(Throwable e){}

        while(true)
        {
            try
            {
                Thread.sleep(interval);

                currentTotalMemory=r.totalMemory()-r.freeMemory();

                if(currentTotalMemory > minUsage+100000)
                {
                    System.gc();
                }

                if(minUsage>currentTotalMemory)
                    minUsage=currentTotalMemory;


            }
            catch(Throwable e){}
        }
    }
}
