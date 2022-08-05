package vabank.spring;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class ThreadPool
{

    private ArrayBlockingQueue<Runnable> taskQueue = null;
    private ArrayList<PoolThread> threads;
    private boolean isStopped = false;
    private int minSize,maxSize,threadNum;
    private long idleTime;

    private AtomicInteger numIdle;

    public ThreadPool(int initialSize,int minSize, int maxSize,long idleTime) throws Exception
    {
        if(initialSize>maxSize || initialSize<minSize || minSize<0 ||maxSize<=0 || minSize>maxSize)throw new Exception("Invalid parameter");


        this.minSize=minSize;
        this.maxSize=maxSize;
        this.idleTime=idleTime;

        taskQueue = new ArrayBlockingQueue<Runnable>(maxSize);
        threads= new ArrayList<PoolThread>(maxSize);

        for(int i=0; i<initialSize; i++)
        {
            threads.add(new PoolThread(this));
        }

        numIdle=new AtomicInteger();
        threadNum=initialSize;

        for(PoolThread thread : threads)
        {
            thread.start();
        }

    }

    public void execute(Runnable task) throws Exception,Throwable
    {
        //jika tidak ada thread yg nganggur dan thread masih bisa berkembang
        //tambah thread
        if(numIdle.get()<=0 && threadNum < maxSize )
        {
            PoolThread thread=new PoolThread(this);
            thread.start();

            synchronized(this)
            {
                threads.add(thread);
                threadNum=threads.size()+1;
            }
        }

        this.taskQueue.put(task);
    }

    public synchronized void stop()
    {
        this.isStopped = true;
        for(PoolThread thread : threads)
        {
            thread.stopTask();
        }
    }


    @SuppressWarnings("element-type-mismatch")
    public boolean stopThreadRequest(PoolThread thread)
    {

        if(minSize >= maxSize || threadNum<=minSize)return false;

        synchronized(this)
        {
            //jika thread pool melebihi minimal size buang thread
            if(threads.size() > minSize)
            {
                threads.remove(thread);
                threadNum=threads.size();
            }
            else
            {
                return false;
            }

            return true;
        }
    }

    public long getIdleTime()
    {
        return idleTime;
    }

    public int getMaxSize()
    {
        return maxSize;
    }

    public int getMinSize()
    {
        return minSize;
    }

    public ArrayBlockingQueue<Runnable> getTaskQueue()
    {
        return taskQueue;
    }

    public int getNumberOfThread()
    {
        return threads.size();
    }

    public int getNumberOfIdleThread()
    {
        return numIdle.get();
    }

    public void addIdleNumber()
    {
        numIdle.getAndIncrement();
    }

    public void subIdleNumber()
    {
        numIdle.getAndDecrement();
    }
}