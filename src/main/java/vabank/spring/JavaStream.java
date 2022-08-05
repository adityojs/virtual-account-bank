package vabank.spring;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import jpa.database.StoredProcedure;
import jpa.util.Context;
import jpa.util.JPABufferedInputStream;
import jpa.util.JPABufferedOutputStream;
import jpa.util.MessageFormat;
import jpa.util.Util;
import jpa.jpos.iso.ISOMsg;
import jpa.jpos.iso.ISOOutOfBoundsException;
import jpa.packager.HTTPRequest;
import org.apache.commons.dbcp.DelegatingConnection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class JavaStream extends Stream {

    private int id;
    private Context ctx;

    private Socket socket;
    private volatile Socket socketToServer;
    private BufferedReader bufferedInputStream;
    private BufferedWriter bufferedOutputStream;

    private JPABufferedInputStream serverInputStream;
    private JPABufferedOutputStream serverOutputStream;

    private ISOMsg iso1,iso2;
    private HTTPRequest httpReq;
    private byte[] byteLength,byteMsg;

    private DocumentBuilder documentBuilder;

    private volatile Connection conn;
    private volatile Statement stmt;
    private String type,key1,key2;
    private boolean running,financialMsg;

    public JavaStream(int id)
    {
        this.id=id;

        ctx=new Context(new StringBuilder());

        ctx.name = "CT " + id;

        iso1=new ISOMsg();
        iso1.setPackager(Main.CLIENT_PACKAGER);


        iso2=new ISOMsg();
        iso2.setPackager(Main.SERVER_PACKAGER);

        byteLength=new byte[2];

        byteMsg=new byte[10240];

        serverInputStream = new JPABufferedInputStream(null);
        serverOutputStream = new JPABufferedOutputStream(null);
    }

    private void endClient()
    {
        try {
            bufferedInputStream.close();
        } catch (Exception ex) {
        }


        try {
            bufferedOutputStream.close();
        } catch (Exception ex) {
        }

        if(socket!=null)
        {
            try {
                socket.close();
            } catch (Exception ex) {
            }
            socket = null;
        }
    }

    private void initClientStream(boolean first)throws Exception
    {
        try
        {
            type="";
            key1="";
            key2="";
            financialMsg=false;

            socketToServer=null;

            conn=null;
            stmt=null;

            ctx.msgId="[MSG_ID-"+Util.getMsgId()+"]";

            if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx, ctx.name).append(this.socket.getLocalSocketAddress())
                    .append(" ").append(this.socket.getRemoteSocketAddress()).append(" | Process client").toString());


            if(!first)return;

            this.socket.setSoTimeout(Main.CLIENT_TIMEOUT);
            this.socket.setKeepAlive(Main.CLIENT_KEEP_ALIVE);

            bufferedInputStream=new BufferedReader(new InputStreamReader(socket.getInputStream()));

            bufferedOutputStream=new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        }
        catch (Exception ex)
        {
            if(Main.USE_LOG_FILE)Context.logger.error(Util.log(ctx, ctx.name).append("Init CT failed :").append(ex).toString());

            throw ex;
        }
    }

    private void connectToDB()throws Exception, Throwable
    {

        conn=Main.DATABASE.getConnection(conn);
        stmt=conn.createStatement();

    }

    private void connectToServer(String ip,int port)throws Exception, Throwable
    {
        if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx,ctx.name).append("Trying connect to server : ").append(ip+":"+port).toString());

        socketToServer = new Socket();
        socketToServer.setReuseAddress(Main.REUSE_ADDRESS);
        try
        {
            socketToServer.setTrafficClass(12);
            socketToServer.setTcpNoDelay(true);
        }
        catch(Exception e){}

        InetSocketAddress address=new InetSocketAddress(ip, port);

        socketToServer.connect(address, Main.CONNECT_SERVER_TIMEOUT);

        socketToServer.setSoTimeout(Main.SERVER_TIMEOUT);
        socketToServer.setKeepAlive(Main.SERVER_KEEP_ALIVE);
    }

    private void closeConnectionToServer()
    {
        if(socketToServer!=null)
        {
            try
            {
                socketToServer=null;
            }
            catch(Exception e){}
        }
    }

    private void closeConnectionToDB()
    {
        if(stmt!=null)
        {
            try{stmt.close();}
            catch(Exception e){}
            stmt=null;
        }

        if(conn!=null)
        {
            try
            {
                conn.close();
            }
            catch(Exception e){}
            conn=null;
        }
    }

    private void communicateWithserver() throws Exception
    {
        try
        {
            serverInputStream.setInputStream(socketToServer.getInputStream());
            serverOutputStream.setOutputStream(socketToServer.getOutputStream());

            int length=0;
            try
            {
                length=iso2.pack(byteMsg);
            }
            catch(ISOOutOfBoundsException e){
                byteMsg=new byte[e.getRequiredLength()];
                length=iso2.pack(byteMsg);
            }

            //log
            String stream=Util.dumpISOMsg(byteMsg, 0, length, ctx);
            if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx, ctx.name).append("[SEND TO SERVER] : ").append(stream).toString());
            if(Main.USE_LOG_ISO)Util.writeLogISO(ctx, stream, "-", key1, type, true);

            //insert log biller
            if(Main.USE_LOG_BILLER)Main.LOG_BILLER.insertLogBiller(type+" req", stream, null, key1,key2, "-", iso1,iso2, ctx.name, ctx);

            //kirim
            MessageFormat.send(serverOutputStream, byteMsg, byteLength, length);

            if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx, ctx.name).append("[Panjang ByteMsg Receiver] : ").append(10240).toString());

            //terima pesan
            length=MessageFormat.receive(serverInputStream, byteMsg, byteLength);

            //log
            stream=Util.dumpISOMsg(byteMsg, 0, length, ctx);
            if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx, ctx.name).append("[RECV FROM SERVER] : ").append(stream).toString());
            if(Main.USE_LOG_ISO)Util.writeLogISO(ctx, stream, iso2.getString(39), key1, type, false);

            //insert log biller
            if(Main.USE_LOG_BILLER)Main.LOG_BILLER.insertLogBiller(type+" resp", stream, null, key1,key2, "-", iso1,iso2, ctx.name, ctx);

            iso2.clear();
            iso2.unpack(byteMsg);
        }
        catch(Exception e)
        {
            {throw e;}
        }
        finally
        {
            try{serverInputStream.close();}
            catch(Exception e){}
            try{serverOutputStream.close();}
            catch(Exception e){}
            socketToServer=null;
        }
    }


    public void stopRunning()
    {
        running=false;
    }

    @Override
    public void run()
    {
        running=true;

        while(running)
        {

            while(true)
            {
                try
                {
                    socket=Main.LISTENER.accept();
                    break;
                }
                catch(Throwable e){}
            }

            boolean first=true;
            do
            {

                //proses pesan
                boolean transactionMsg=true;
                boolean validIso=true;
                try
                {
                    initClientStream(first);
                    first=false;

                    //terima pesan dari client
                    httpReq=MessageFormat.readHTTP(bufferedInputStream);

                    if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx, ctx.name).append("Request Line : ").append(httpReq.reqLine).toString());
                    if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx, ctx.name).append("Receive : ").append(httpReq.body).toString());

                    iso1.clear();
                    iso2.clear();
                    httpToIso();
                    validIso=true;

                    //proses pesan
                    if(transactionMsg)
                    {
                        if(financialMsg)
                        {
                            key1=iso1.getString(48).substring(26, 46);
                            key2="";
                            ctx.msgId+=" | "+key1;
                        }
                        else key1=key2="";

                        if(Main.USE_LOG_ISO)Util.writeLogISO(ctx, httpReq.body, "-", key1,type,true);

                        processMsg();
                    }
                }
                catch (Throwable e)
                {
                    if(Main.USE_LOG_FILE)Context.logger.error(Util.log(ctx, ctx.name).append("failed process msg : ").append(e).toString(),e);
                }

                //balas pesan
                try
                {
                    if(validIso && httpReq.code!=null)
                    {
                        MessageFormat.sendHTTP(httpReq, bufferedOutputStream);

                        if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx,ctx.name).append("client RC : ").append(httpReq.rc).toString());
                        if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx,ctx.name).append("Send : ").append(httpReq.content).toString());

                        if(transactionMsg && Main.USE_LOG_ISO)Util.writeLogISO(ctx, httpReq.content, httpReq.rc, key1,type,false);
                    }
                }
                catch(Throwable e)
                {
                    if(Main.USE_LOG_FILE)Context.logger.error(Util.log(ctx,ctx.name).append("failed response msg : ").append(e).toString());
                }
                finally
                {
                    closeConnectionToDB();
                }

            }while(Main.CONNECTION_FULL);

            try{endClient();}
            catch(Exception e){}

        }
    }

    private void processMsg()throws Exception,Throwable
    {
        if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx,ctx.name).append("proses ").append(type).toString());

        //panggil sp1
        String spKey=Util.clr(ctx.sb)
                .append(iso1.getMTI()).append("_")
                .append(iso1.getString(3)).toString();

        StoredProcedure sp1=Main.SP_MAPPING.get("SQLA_"+spKey);
        if(sp1==null)
        {
            return;
        }

        connectToDB();
        ResultSet rs=callStoredProcedure(sp1);
        HashMap<String,String> others=Main.DATABASE.parseResultSet(rs, iso1, iso2, ctx);
        closeConnectionToDB();

        if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx,ctx.name).append("result SP1 : ").append(iso2.getString(39)).toString());

        if(Main.SP_MAPPING.get("SQLB_"+spKey)==null ||
                (others.containsKey("TO_BILLER") && others.get("TO_BILLER").equals("0")))
        {
            httpReq.code="200";
            httpReq.codeStr="OK";
            httpReq.rc=others.get("RC");
            httpReq.content=others.get("BODY");

            return;
        }

        //jika sp1 sukses
        if(iso2.getString(39).equalsIgnoreCase("00"))
        {
            iso2.unset(39);

            boolean isCon=false;
            try
            {
                connectToServer(others.get("IP"),Integer.parseInt(others.get("PORT")));
                isCon=true;
                communicateWithserver();
            }
            catch(Throwable e)
            {
                if(Main.USE_LOG_FILE)Context.logger.error(Util.log(ctx,ctx.name).append("failed communicate with server : ").append(e).toString(),e);

                if(!isCon)iso2.set(39,"13");
                else if(e instanceof SocketTimeoutException)iso2.set(39,"18");
                else iso2.set(39,"06");

            }

            if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx,ctx.name).append("[SERVER RC] : ").append(iso2.getString(39)).toString());

            //panggil sp2
            StoredProcedure sp2=Main.SP_MAPPING.get("SQLB_"+spKey);

            connectToDB();
            rs=callStoredProcedure(sp2);
            others=Main.DATABASE.parseResultSet(rs, iso1, iso2, ctx);
            closeConnectionToDB();

            String rc=others.get("RC");

            if(Main.USE_LOG_FILE)Context.logger.info(Util.log(ctx,ctx.name).append("result SP2 : ").append(rc).toString());

            if(rc!=null && rc.equals("")==false)
            {
                httpReq.code="200";
                httpReq.codeStr="OK";
                httpReq.rc=others.get("RC");
                httpReq.content=others.get("BODY");
            }
        }
        else
        {
            closeConnectionToServer();

            httpReq.code="200";
            httpReq.codeStr="OK";
            httpReq.rc=others.get("RC");
            httpReq.content=others.get("BODY");
        }
    }

    private ResultSet callStoredProcedure(StoredProcedure sp)throws Throwable
    {
        try
        {
            return Main.DATABASE.callStoredProcedure(sp, iso1, iso2, stmt, ctx);
        }
        catch(SQLException e)
        {
            if(e.getSQLState()!=null && e.getSQLState().startsWith("08"))
            {
                if(Main.USE_LOG_FILE)Context.logger.error(Util.log(ctx,ctx.name).append("Failed call SQL,retry call : ").append(e).toString());

                long start=System.currentTimeMillis();
                while(true)
                {
                    try
                    {
                        //jika dbcp,close dulu inner connectionnya
                        if(conn instanceof DelegatingConnection)
                        {
                            ((DelegatingConnection) conn).getInnermostDelegate().close();
                        }

                        closeConnectionToDB();
                        connectToDB();
                        return Main.DATABASE.callStoredProcedure(sp, iso1, iso2, stmt, ctx);
                    }
                    catch(SQLException ex)
                    {
                        if(e.getSQLState()==null || !e.getSQLState().startsWith("08") ||
                                System.currentTimeMillis() - start > Main.DATABASE.getMaxWait())
                            throw ex;
                        else if(Main.USE_LOG_FILE)Context.logger.error(Util.log(ctx,ctx.name).append("Failed call SQL again, retry : ").append(ex).toString());
                    }
                }
            }
            else throw e;
        }
    }

    private void httpToIso()throws Exception
    {
        if(httpReq.reqLine.contains("wsdl"))
        {
            httpReq.code="200";
            httpReq.codeStr="OK";
            httpReq.content=Util.getWSDL();
            throw new Exception("Client req WSDL");
        }
        
        /*if(httpReq.reqLine.toLowerCase().contains("billpayment")==false)
        {
            httpReq.code="404";
            httpReq.codeStr="NOTFOUND";
            httpReq.content="";
            throw new Exception("Client req not found");
        }*/

        Element root=getRootOfXml(httpReq.body);

        StringBuilder sb=new StringBuilder();

        if(root.getElementsByTagNameNS("*", "inquiry").getLength()>0)
        {
            type="inq";
            iso1.setMTI("0200");
            iso1.set(3,"380000");

            NodeList nl=root.getElementsByTagNameNS("*", "language");
            String content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 2));

            nl=root.getElementsByTagNameNS("*", "trxDateTime");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 10));

            nl=root.getElementsByTagNameNS("*", "transmissionDateTime");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 10));

            nl=root.getElementsByTagNameNS("*", "companyCode");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 5));

            nl=root.getElementsByTagNameNS("*", "channelID");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 1));

            nl=root.getElementsByTagNameNS("*", "billKey1");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 20));

            nl=root.getElementsByTagNameNS("*", "billKey2");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 20));
            iso1.set(4,content);

            nl=root.getElementsByTagNameNS("*", "billKey3");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 20));

            nl=root.getElementsByTagNameNS("*", "reference1");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 32));

            nl=root.getElementsByTagNameNS("*", "reference2");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 32));

            nl=root.getElementsByTagNameNS("*", "reference3");
            content=nl.getLength()>0 ? nl.item(0).getTextContent() : "";
            sb.append(Util.padRight(ctx, ' ', content, 32));
        }

    }

    private Element getRootOfXml(String xmlString)throws Exception
    {
        if(documentBuilder==null)
        {
            DocumentBuilderFactory fact=DocumentBuilderFactory.newInstance();
            fact.setNamespaceAware(true);
            documentBuilder = fact.newDocumentBuilder();
        }
        else documentBuilder.reset();

        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(xmlString));
        Document doc=documentBuilder.parse(is);
        Element root=(Element)doc.getDocumentElement();

        return root;
    }

}
