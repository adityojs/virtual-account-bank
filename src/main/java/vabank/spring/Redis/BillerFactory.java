import jatelindo.pool.PoolableObjectFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class BillerFactory implements PoolableObjectFactory<BillerFactory>
{
    private AtomicInteger id=new AtomicInteger();

    public LogBillerPackages makeObject()
    {
        return new LogBillerPackages(getId());
    }

    public void destroyObject(LogBillerPackages obj)
    {
        obj=null;
    }


    private int getId()
    {
        while(true)
        {
            int old=id.get();
            int inc = old+1 > 99999999 ? 1 : old+1;

            if(id.compareAndSet(old, inc))
                return inc;
        }
    }

}
