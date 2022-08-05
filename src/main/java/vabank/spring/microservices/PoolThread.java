package vabank.spring;

import java.util.concurrent.TimeUnit;

public class PoolThread extends Thread
{
    private ThreadPool threadPool;
    private boolean isStopped = false;

    public PoolThread(ThreadPool threadPool)
    {
        this.threadPool=threadPool;
    }

    public void run()
    {
        Thread.currentThread().setName("Thread PoolThread");

        while(!isStopped())
        {
            try
            {
                threadPool.addIdleNumber();

                Runnable runnable = threadPool.getTaskQueue().poll(threadPool.getIdleTime(), TimeUnit.MILLISECONDS);

                threadPool.subIdleNumber();

                if(runnable==null)
                {
                    boolean approve=threadPool.stopThreadRequest(this);
                    if(approve)stopTask();
                }
                else
                {
                    runnable.run();
                }
            }
            catch(Throwable e)
            {
            }
        }
    }



    public synchronized void stopTask()
    {
        isStopped = true;
        this.interrupt(); //break pool thread out of dequeue() call.

    }

    public synchronized boolean isStopped()
    {
        return isStopped;
    }
}
