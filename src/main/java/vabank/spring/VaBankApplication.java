package vabank.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import jatelindo.pool.GarbageCollector;
import jatelindo.pool.ThreadPool;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.sql.Connection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import jpa.database.Database;
import jpa.database.StoredProcedure;
import jpa.logbiller.LogBiller;
import jpa.packager.JPAPackager;
import jpa.util.Context;
import jpa.util.Util;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import jpa.jpos.iso.ISOBasePackager;
import jpa.packager.ISO1987APackagerJ;
import jpa.packager.ISO1987APackagerM;
import org.apache.commons.dbcp.BasicDataSource;
	

@SpringBootApplication
public class VaBankApplication {

	public static ServerSocket LISTENER;
	public static GarbageCollector GARBAGE_COLLECTOR;
	public static Database DATABASE;
	public static LogBiller LOG_BILLER;

	public static ClientThread[] CLIENT_THREAD;

	public static HashMap<String,StoredProcedure> SP_MAPPING;
	public static ThreadPool PIPELINING_POOL;

	public static int CLIENT_TIMEOUT;
	public static int SERVER_TIMEOUT;
	public static int CONNECT_SERVER_TIMEOUT;
	public static boolean CLIENT_KEEP_ALIVE, SERVER_KEEP_ALIVE,REUSE_ADDRESS,
			USE_LOG_BILLER,USE_LOG_FILE,USE_LOG_ISO,CONNECTION_FULL;

	public static ISOBasePackager SERVER_PACKAGER,CLIENT_PACKAGER;
	public static JPAPackager SERVER_JPA_PACKAGER,CLIENT_JPA_PACKAGER;


	//PROPERTIES UMUM
	private Properties properties,spProperties;
	private Context ctx;

	public static void main(String[] args) {
		SpringApplication.run(VaBankApplication.class, args);


		try
		{
			//System.out.println("[v1.1 ADT]-Perubahan u/ adopsi infant dari KAI");
			Main m=new Main();

			m.ctx=new Context(new StringBuilder());
			m.ctx.name="Main";

			m.loadProperties();

			m.loadConfiguration();
			m.loadLog();
			m.loadStoredProcedure();
			m.loadDatabase();
			m.loadClient();

			if(Main.USE_LOG_FILE)Context.logger.info(Util.log(m.ctx, m.ctx.name).append("AUTHOR BY ADITYO JAYA SUBAKTI").toString());

			//GATEWAY PROPERTIES
			int gatewayPort=Integer.parseInt(m.properties.getProperty("gateway_port"));
			int listenerQueueLength=Integer.parseInt(m.properties.getProperty("listener_queue_length"));
			LISTENER = new ServerSocket(gatewayPort, listenerQueueLength);
			LISTENER.setReuseAddress(Main.REUSE_ADDRESS);


			for(int i=0;i<CLIENT_THREAD.length;i++)
			{
				CLIENT_THREAD[i].start();
			}

		}
		catch(Throwable e)
		{
			System.out.println("FATAL ERROR, SHUTDOWN : "+e);
			e.printStackTrace();
			System.exit(0);
		}
	}

	private void loadProperties()throws Exception
	{
		properties=new Properties();
		properties.load(new FileInputStream("Properties/config.properties"));

		spProperties=new Properties();
		spProperties.load(new FileInputStream("Properties/storedprocedure.properties"));
	}

	private void loadLog()throws Exception
	{
		if(USE_LOG_FILE)Context.logger = Logger.getLogger("umum");
		Context.fatalLogger =Logger.getLogger("penting");
		if(USE_LOG_ISO)Context.isoLogger =Logger.getLogger("iso");
		DOMConfigurator.configure("Properties/log.xml");
	}

	private void loadClient()throws Exception
	{
		//CLIENT CONNECTION PROPERTIES
		int numberClient=Integer.parseInt(properties.getProperty("number_client"));

		Main.CLIENT_THREAD=new ClientThread[numberClient];
		for(int i=0;i<numberClient;i++)
		{
			CLIENT_THREAD[i]=new ClientThread(i+1);
		}

		int pipeliningThread=Integer.parseInt(properties.getProperty("pipelining_thread"));
		if(pipeliningThread>0)PIPELINING_POOL=new ThreadPool(pipeliningThread, pipeliningThread, pipeliningThread, 3600000);
	}

	private void loadDatabase()throws Exception
	{
		int library=Integer.parseInt(properties.getProperty("library_database"));


		if(library==0)//boneCP
		{
			Class.forName("com.mysql.jdbc.Driver");

			BoneCPConfig config=new BoneCPConfig();
			config.setAcquireIncrement(5);
			config.setAcquireRetryAttempts(0);
			config.setAcquireRetryDelayInMs(50);
			config.setCloseConnectionWatch(false);
			config.setConnectionHook(null);
			config.setDisableConnectionTracking(true);
			config.setConnectionTestStatement("SELECT 1 FROM DUAL");
			config.setConnectionTimeoutInMs(Long.parseLong(properties.getProperty("max_wait_database")));
			config.setDefaultAutoCommit(Boolean.parseBoolean(properties.getProperty("auto_commit_database")));
			config.setIdleMaxAge(Long.parseLong(properties.getProperty("minEvictableIdleTimeMillis_database")), TimeUnit.MILLISECONDS);
			config.setInitSQL(null);
			config.setJdbcUrl(properties.getProperty("url_database"));
			config.setLazyInit(false);
			config.setMaxConnectionAgeInSeconds(Long.parseLong(properties.getProperty("minEvictableIdleTimeMillis_database"))/1000);
			config.setMaxConnectionsPerPartition(Integer.parseInt(properties.getProperty("maxactive_database"))/2);
			config.setMinConnectionsPerPartition(Integer.parseInt(properties.getProperty("minidle_database"))/2);
			config.setPartitionCount(2);
			config.setPassword(properties.getProperty("password_database"));
			config.setPoolAvailabilityThreshold(Integer.parseInt(properties.getProperty("minidle_database"))*100/Integer.parseInt(properties.getProperty("maxactive_database"))/2);
			config.setQueryExecuteTimeLimitInMs(Long.parseLong(properties.getProperty("max_wait_database")));
			config.setReleaseHelperThreads(0);
			config.setServiceOrder("LIFO");
			config.setStatementReleaseHelperThreads(0);
			config.setStatementsCacheSize(0);
			config.setStatisticsEnabled(false);
			config.setUsername(properties.getProperty("username_database"));
			config.setDisableJMX(true);

			//config baru
			config.setTransactionRecoveryEnabled(false);
			config.setLogStatementsEnabled(false);

			config.setIdleConnectionTestPeriod(0,TimeUnit.MILLISECONDS);

			BoneCP boneCP=new BoneCP(config);
			DATABASE=new Database(boneCP);

			int testOn=Integer.parseInt(properties.getProperty("test_on_database"));
			if(testOn==0)DATABASE.setTestOnBorrow(true);
			else if(testOn==1)config.setIdleConnectionTestPeriod(Long.parseLong(properties.getProperty("timeBetweenEvictionRunsMillis_database")),TimeUnit.MILLISECONDS);

			int initial=Integer.parseInt(properties.getProperty("initialactive_database"));
			Connection conn[]=new Connection[initial];
			for(int i=0;i<initial;i++)
			{
				try{conn[i]=DATABASE.getConnection(null);}
				catch(Exception e){}
			}
			for(int i=0;i<initial;i++)
			{
				if(conn[i]!=null)conn[i].close();
			}
		}
		else
		{

			BasicDataSource bds=new BasicDataSource();
			bds.setDriverClassName(properties.getProperty("driver_database"));
			bds.setUrl(properties.getProperty("url_database"));
			bds.setUsername(properties.getProperty("username_database"));
			bds.setPassword(properties.getProperty("password_database"));
			bds.setMaxActive(Integer.parseInt(properties.getProperty("maxactive_database")));
			bds.setMaxIdle(Integer.parseInt(properties.getProperty("maxidle_database")));
			bds.setInitialSize(Integer.parseInt(properties.getProperty("minidle_database")));
			bds.setMinIdle(Integer.parseInt(properties.getProperty("initialactive_database")));
			bds.setAccessToUnderlyingConnectionAllowed(true);

			bds.setDefaultAutoCommit(Boolean.parseBoolean(properties.getProperty("auto_commit_database")));
			bds.setMaxWait(Long.parseLong(properties.getProperty("max_wait_database")));
			bds.setMinEvictableIdleTimeMillis(Long.parseLong(properties.getProperty("minEvictableIdleTimeMillis_database")));
			bds.setTimeBetweenEvictionRunsMillis(Long.parseLong(properties.getProperty("timeBetweenEvictionRunsMillis_database")));

			bds.setValidationQuery("SELECT 1 FROM DUAL");
			bds.setTestOnBorrow(false);
			int testOn=Integer.parseInt(properties.getProperty("test_on_database"));
			if(testOn==0)bds.setTestOnBorrow(true);
			else if(testOn==1)bds.setTestWhileIdle(true);
			else if(testOn==2)bds.setTestOnReturn(true);

			DATABASE=new Database(bds);

			DATABASE.getConnection(null).close();
		}
	}

	private void loadStoredProcedure()throws Exception
	{
		SP_MAPPING=new HashMap<String, StoredProcedure>();

		// Enumerate all system properties
		Enumeration en = spProperties.keys();
		for (; en.hasMoreElements(); ) {
			// Get property name
			String propName = (String)en.nextElement();

			// Get property value
			String propValue = (String)spProperties.get(propName);

			SP_MAPPING.put(propName, new StoredProcedure(propValue));
		}

		LOG_BILLER.setSpLogBiller(SP_MAPPING.get("LOG_BILLER"));

	}

	private void loadConfiguration()throws Exception
	{
		CLIENT_TIMEOUT=Integer.parseInt(properties.getProperty("client_timeout"));
		SERVER_TIMEOUT=Integer.parseInt(properties.getProperty("server_timeout"));
		CONNECT_SERVER_TIMEOUT=Integer.parseInt(properties.getProperty("connect_server_timeout"));
		CONNECTION_FULL=Boolean.parseBoolean(properties.getProperty("connection_full"));
		CLIENT_KEEP_ALIVE=Boolean.parseBoolean(properties.getProperty("client_keep_alive"));
		SERVER_KEEP_ALIVE=Boolean.parseBoolean(properties.getProperty("server_keep_alive"));
		REUSE_ADDRESS=Boolean.parseBoolean(properties.getProperty("reuse_socket"));

		CLIENT_PACKAGER=new ISO1987APackagerM();
		CLIENT_JPA_PACKAGER=(ISO1987APackagerM)CLIENT_PACKAGER;

		SERVER_PACKAGER=new ISO1987APackagerJ();
		SERVER_JPA_PACKAGER=(ISO1987APackagerJ)SERVER_PACKAGER;

		USE_LOG_BILLER=Boolean.parseBoolean(properties.getProperty("log_biller"));
		USE_LOG_FILE=Boolean.parseBoolean(properties.getProperty("log_file"));
		USE_LOG_ISO=Boolean.parseBoolean(properties.getProperty("log_iso"));

		//if(USE_LOG_BILLER)
		//{
		int lbCapacity=Integer.parseInt(properties.getProperty("log_biller_capacity"));
		long lbInterval=Long.parseLong(properties.getProperty("log_biller_interval"));

		LOG_BILLER=new LogBiller(lbCapacity, lbInterval, (int)(lbCapacity*0.1), 3600000,Integer.parseInt(properties.getProperty("gateway_port")));
		//}
	}

}
