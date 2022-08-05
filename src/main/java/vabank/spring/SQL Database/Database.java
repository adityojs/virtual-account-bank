package vabank.spring;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.ConnectionHandle;
import java.net.InetAddress;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import jpa.jpos.iso.ISOMsg;
import jpa.packager.JPAPackager;
import jpa.util.Context;
import jpa.util.Util;
import org.apache.commons.dbcp.BasicDataSource;


public class Database {

    private BasicDataSource bds;
    private BoneCP bcp;
    private String name,hostAddress,hostName;
    private boolean testOnBorrow;

    private Database()
    {
        this.name="Database";
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
    }

    public Database(BasicDataSource bds)
    {
        this();
        this.bds=bds;
        this.bcp=null;
    }

    public Database(BoneCP bcp)
    {
        this();
        this.bds=null;
        this.bcp=bcp;
    }

    public void setTestOnBorrow(boolean value){testOnBorrow=value;}

    public Connection getConnection(Connection conn) throws Exception
    {
        if(conn!=null)
        {
            return conn;
        }

        conn= bcp==null ? bds.getConnection() : bcp.getConnection();

        if(testOnBorrow && bcp!=null)//jika test on borrw pada bcp
        {
            long start=System.currentTimeMillis();

            while(true)
            {
                try
                {
                    ConnectionHandle connInner=(ConnectionHandle)conn;
                    boolean alive=bcp.isConnectionHandleAlive(connInner);
                    if(alive)break;

                    //close dan ambil ulang
                    connInner.close();
                    conn=bcp.getConnection();
                }catch(Exception e){}
                finally
                {
                    if(System.currentTimeMillis()-start > bcp.getConfig().getConnectionTimeoutInMs())
                        throw new SQLException("Get connection BoneCP, is timed out");
                }
            }
        }

        return conn;
    }

    public ResultSet callStoredProcedure(StoredProcedure sp,ISOMsg msg1,ISOMsg msg2,Statement stmt,Context ctx)throws Exception
    {
        try
        {
            //extract value params
            SPParameter spParams[]=sp.getParams();

            //parse query untuk log
            StringBuilder sb=ctx.sb;
            Util.clr(sb).append("{").append(sp.getSpName()).append("(");

            for(int i=0;i<spParams.length;i++)
            {
                sb.append('\'');

                if(spParams[i].tipe==1 && msg1.hasField(spParams[i].field))
                        sb.append(msg1.getString(spParams[i].field));
                else if(spParams[i].tipe==2 && msg2.hasField(spParams[i].field))
                        sb.append(msg2.getString(spParams[i].field));
                sb.append('\'');

                if(i<spParams.length-1)sb.append(',');
                else sb.append(")}");
            }

            String logQuery=handle_backslash_adit(sb.toString(),sb,true);

            if(Context.logger!=null)Context.logger.info(Util.log(ctx, name).append(logQuery).toString());


            return stmt.executeQuery(logQuery);
        }
        catch(Exception e)
        {
            Context.logger.error(Util.log(ctx, name).append(sp.getSpName()).append(" : ").append(e).toString());
            throw e;
        }

    }


    public HashMap<String,String> parseResultSet(ResultSet rs,ISOMsg isomsg,ISOMsg isomsg2,Context ctx)throws Exception
    {
        try
        {
            HashMap<String,String> temp=null;

            if(rs.first())
            {
                ResultSetMetaData meta=rs.getMetaData();

                for(int i=1,max=meta.getColumnCount();i<=max;i++)
                {
                    String columnName=meta.getColumnLabel(i);

                    boolean ada=false;
                    if(columnName.startsWith("1_")||columnName.startsWith("2_"))//bukan default
                    {
                        String iso=columnName.substring(0,1);
                        int index=Integer.parseInt(columnName.substring(2));
                        if(iso.equals("1"))
                        {
                            if(rs.getString(i)==null)isomsg.set(index, "");
                            else if(!rs.getString(i).equals("null"))isomsg.set(index, rs.getString(i));
                            else isomsg.unset(index);
                            ada=true;
                        }
                        else if(iso.equals("2"))
                        {
                            if(rs.getString(i)==null)isomsg2.set(index, "");
                            else if(!rs.getString(i).equals("null"))isomsg2.set(index, rs.getString(i));
                            else isomsg2.unset(index);
                            ada=true;
                        }
                    }

                    if(!ada)
                    {
                        if(temp==null)temp=new HashMap<String, String>();
                        temp.put(columnName, rs.getString(i));
                    }

                }
            }

            return temp;
        }
        catch(Exception e)
        {
            if(Context.logger!=null)Context.logger.error(Util.log(ctx, name).append("failed parse resultset").append(" : ").append(e).toString());

            throw e;
        }
        finally
        {
            try{rs.close();}
            catch(Exception e){}
        }

    }

    //tambah ke tabel biller log
    public void insertLogBiller(Connection conn,String type,String key1,String key2,String rc,
            String data48,String tid,String tgl,String stream,String nama,Context ctx) throws Exception
    {
        CallableStatement cs=null;

        try
        {
            stream = handle_backslash_adit(stream,ctx.sb,false);
            data48 = "";

            cs=conn.prepareCall("{call log_biller_postpaid(?, ?, ?, ?, ?, ?, ?, ?, ?)}");
            cs.setString(1, key1);
            cs.setString(2, key2);
            cs.setString(3, type);
            cs.setString(4, rc);
            cs.setString(5, tid);
            cs.setString(6, data48);
            cs.setString(7, stream);
            cs.setString(8, tgl);
            cs.setString(9, Util.clr(ctx.sb).append("[").append(hostAddress).append("(").append(hostName).append(")").append(nama).append("]").toString());
            cs.execute();
        }
        catch(Exception e){throw e;}
        finally
        {
            if(cs!=null)
            {
                try{cs.close();}
                catch(Exception e){}
            }
        }
    }


    public static String handle_backslash_adit(String teks,StringBuilder sb,boolean semua)
    {
        sb=Util.clr(sb);

        for(int i=0;i<teks.length();i++)
        {
            char a=teks.charAt(i);
            if(a=='\\')
                sb.append('\\');
            else if(a=='\'')
            {
                char prev=teks.charAt(i-1);
                char next=teks.charAt(i+1);

                if(semua && prev!=',' && next!=',' && prev!='(' && next!=')')
                    sb.append('\\');
                else if(!semua)sb.append('\\');
            }

            sb.append(a);


        }

       return sb.toString();

        /*teks=teks.replaceAll("\\\\", " ");
        teks=teks.replaceAll("/", " ");
        return teks;*/
    }

    public long getMaxWait()
    {
        return bds==null ? bcp.getConfig().getConnectionTimeoutInMs() : bds.getMaxWait();
    }


}