package vabank.spring;

import jatelindo.pool.ObjectPool;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentLinkedQueue;
import jpa.apps.Main;
import jpa.database.Database;
import static jpa.database.Database.handle_backslash_adit;
import jpa.database.SPParameter;
import jpa.database.StoredProcedure;
import jpa.jpos.iso.ISOMsg;
import jpa.util.Context;
import jpa.util.Util;


public class BillerGrid extends Thread
{
    private static final String SP_BILLER_GRID="insert_biller_grid";

    private static SimpleDateFormat formatDate=new SimpleDateFormat("yyyyMMddHHmmss");

    private ConcurrentLinkedQueue<String> billerQueue;

    private long interval;

    private Context ctx;

    private volatile boolean sleep;

    private static String LOKASI;
    private StoredProcedure BillerGrid;

    public BillerGrid(int queueCapacity,long interval,int initSize,long idleTime,int portListening) throws Exception
    {

        ctx=new Context(new StringBuilder());
        ctx.name="BillerGrid";

        String hostAddress,hostName;

        try
        {
            InetAddress addr = InetAddress.getLocalHost();
            hostAddress=addr.getHostAddress();
            hostName=addr.getHostName();

        }
        catch(Exception e)
        {
            hostAddress="X.X.X.X";
            hostName="localhost";
        }

        LOKASI=Util.clr(ctx.sb).append("[").append(hostAddress).append("(").append(hostName).append(":").append(portListening).append(")").append("]").toString();

        if(Main.USE_BILLER_GRID)billerQueue=new ConcurrentLinkedQueue<String>();

        this.interval=interval;

        if(Main.USE_BILLER_GRID)this.start();
    }

    public void setBillerGrid(SQL sql)
    {
        this.BillerGrid=sql;
    }

    private String createQueryString(String[] data0, ISOMsg msg1, ISOMsg msg2,Context ctx)
    {
        //extract value params
        SPParameter spParams[]=BillerGrid.getParams();

        //parse query untuk BillerGrid
        StringBuilder sb=ctx.sb;
        Util.clr(sb).append("{").append(BillerGrid.getSpName()).append("(");

        for(int i=0;i<spParams.length;i++)
        {
            sb.append('\'');

            if(spParams[i].tipe==0 && data0.length>spParams[i].field)
                sb.append(handle_backslash_adit(data0[spParams[i].field], new StringBuilder(128), false));
            else if(spParams[i].tipe==1 && msg1.hasField(spParams[i].field))
                sb.append(handle_backslash_adit(msg1.getString(spParams[i].field), new StringBuilder(128), false));
            else if(spParams[i].tipe==2 && msg2.hasField(spParams[i].field))
                sb.append(handle_backslash_adit(msg2.getString(spParams[i].field), new StringBuilder(128), false));
            sb.append('\'');

            if(i<spParams.length-1)sb.append(',');
            else sb.append(")}");
        }

        return (sb.toString());
    }

    public void insertBillerGrid(String type,String stream,String data48,String key1,String key2,String rc,
                                ISOMsg msg1,ISOMsg msg2,String name,Context ctx)
    {
        try
        {

            //hilangkan backslash pada stream
            StringBuilder sb=ctx.sb;

            String[] data0=new String[6];
            data0[0]=key1; data0[1]=key2; data0[2]=type;
            data0[3]=stream; data0[4]=formatDate.format(System.currentTimeMillis()); data0[5]=LOKASI;

            String query=createQueryString(data0, msg1, msg2, ctx);

            if(Context.logger!=null)Context.logger.info(Util.log(ctx, name).append(query).toString());

            if(!Main.USE_BILLER_GRID)return;

            billerQueue.add(query);

            if(sleep)this.interrupt();
        }
        catch (Throwable ex)
        {
            if(Context.logger!=null)Context.logger.error(Util.log(ctx, ctx.name).append("failed queue BillerGrid : ").append(ex).toString());
        }
    }

    public void run()
    {
        String tempPackage=null;
        Connection conn=null;
        Statement stmt=null;
        while(true)
        {
            try
            {
                sleep=true;
                try
                {
                    Thread.sleep(interval);
                }catch(Exception e){}
                sleep=false;

                conn=null;
                stmt=null;

                while((tempPackage=billerQueue.poll())!=null)
                {
                    try
                    {
                        if(conn==null)
                        {
                            conn=Main.DATABASE.getConnection(conn);
                            stmt=conn.createStatement();
                        }

                        stmt.execute(tempPackage);

                    }
                    catch(Throwable e)
                    {
                        Context.fatalLogger.fatal(Util.log(ctx, ctx.name).append("Failed insert Biller Grid : ").append(tempPackage).toString(), e);

                        if(e instanceof SQLException)break;
                    }

                }
            }
            catch(Throwable e){}
            finally
            {
                if(stmt!=null)
                {
                    try{stmt.close();}
                    catch(Exception e){}
                }
                if(conn!=null)
                {
                    try{conn.close();}
                    catch(Exception e){}
                }
            }
        }
    }

}