package vabank.spring;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectPool<E> extends Thread
{
    private ConcurrentLinkedQueue<E> objects=null;

    private PoolableObjectFactory<E> factory;
    private boolean isStopped = false;
    private int minSize,maxSize;
    private long idleTime;

    private ConcurrentHashMap<E,Long> insertTimeMap;
    private AtomicInteger objectSize;
    private AtomicInteger size;


    public ObjectPool(PoolableObjectFactory<E> factory,int initialSize,int minSize, int maxSize,long idleTime)throws Exception
    {
        if( ( maxSize>0 &&  initialSize>maxSize) ||
                initialSize<minSize ||
                minSize<0
                || ( maxSize>0 &&  minSize>maxSize))throw new Exception("Invalid parameter");

        this.minSize=minSize;
        this.maxSize=maxSize;
        this.idleTime=idleTime;
        this.factory=factory;

        objects=new ConcurrentLinkedQueue<E>();
        insertTimeMap=new ConcurrentHashMap<E, Long>(initialSize);

        for(int i=0; i<initialSize; i++)
        {
            E e=factory.makeObject();
            objects.add(e);

            insertTimeMap.put(e,System.currentTimeMillis());
        }

        objectSize=new AtomicInteger(initialSize);
        size=new AtomicInteger(initialSize);

        if(idleTime<0 || minSize<maxSize || maxSize<=0)
        {
            this.start();
        }

    }

    public E borrowObject() throws Exception,Throwable
    {
        E e=null;

        //jika pool kosong dan masih bisa membuat object
        if(size.get()<=0 && (objectSize.get()<maxSize || maxSize<=0 ))
        {
            e=factory.makeObject();
            objectSize.incrementAndGet();
        }
        else
        {
            e=objects.poll();
            if(e==null)
            {
                e=factory.makeObject();
                objectSize.incrementAndGet();
            }
            else
            {
                size.decrementAndGet();
                if(minSize<maxSize || maxSize<0)insertTimeMap.remove(e);
            }
        }

        return e;

    }

    public boolean isObjectAvailable()
    {
        return size.get()>0 || objectSize.get()<maxSize || maxSize<=0;
    }

    public void returnObject(E e) throws Exception
    {
        objects.add(e);

        if(minSize<maxSize || maxSize<=0)insertTimeMap.put(e, System.currentTimeMillis());

        size.incrementAndGet();
    }

    public synchronized void stopPool()
    {
        this.isStopped = true;
        interrupt();
        E e=null;
        while((e=objects.poll())!=null)
        {
            factory.destroyObject(e);
        }
    }

    public void run()
    {

        Thread.currentThread().setName("Thread ObjectPool");

        while(!isStopped)
        {

            try
            {
                Thread.sleep(idleTime);
            }
            catch(Exception e){}

            try
            {
                if(isStopped || objectSize.get()<=minSize || size.get()<=0)continue;

                long now=System.currentTimeMillis();
                Long insert=null;
                E e=null;

                while(objectSize.get()>minSize && !objects.isEmpty())
                {
                    e=objects.poll();

                    if(e!=null &&
                            ((insert=insertTimeMap.get(e))==null || now-insert > idleTime))//buang object
                    {
                        size.decrementAndGet();
                        objectSize.decrementAndGet();
                        insertTimeMap.remove(e);
                        factory.destroyObject(e);
                    }
                    else
                    {
                        returnObject(e);
                    }
                }

            }
            catch(Throwable e)
            {

            }
        }
    }

    public int getObjectCount(){return objectSize.get();}
    public int getObjectIdle(){return objects.size();}
    public int getObjectActive(){return objectSize.get()-objects.size();}
}
