package com.osbs.usermodel.hadoop.tools;

import java.io.File;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.osbs.usermodel.tools.LoadConfigurations;
import com.osbs.utils.MyLogger;
import com.osbs.utils.MyUtils;
import com.osbs.utils.SSHClient;

public class HDFSFileCopier 
{
	
	private final static String sourcesTypeLocal = "LOCAL";
	private final static String sourcesTypeRemote = "REMOTE";
	
	MyLogger logger = MyLogger.getInstance();
	
	String sourceData = null;
	String sourceServers = null;
	String sourceCalendar = null;
	String sourceWorkinghours = null;
	String sourceType = null;
	String hdfsData = null;
	
	String hdfsUser = null;
	String hdfsFqdn = null;
	String hdfsPort = null;
	
	String systemUser = null;
	String systemPass = null;
	String systemHost = null;
	

	public HDFSFileCopier(String config) 
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HDFSFileCopier");
		
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.usermodelConfigType, config);
		sourceData = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "source.data");
		sourceServers = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "source.servers");
		sourceCalendar = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "source.calendar");
		sourceWorkinghours = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "source.workinghours");
		sourceType = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "source.type");
		hdfsData =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "hdfs.data");
		hdfsUser =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "hdfs.user");
		hdfsFqdn =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "hdfs.fqdn");
		hdfsPort =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "hdfs.port");
		systemUser =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "system.user");
		systemPass =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "system.pass");
		systemHost =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "system.host");
		
		if (logger.isDebug())
		{
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: sourceData::["+sourceData+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: sourceServers::["+sourceServers+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: sourceCalendar::["+sourceCalendar+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: sourceWorkinghours::["+sourceWorkinghours+"]");					
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: sourceType::["+sourceType+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: hdfsData::["+hdfsData+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: hdfsUser::["+hdfsUser+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: hdfsFqdn::["+hdfsFqdn+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: hdfsPort::["+hdfsPort+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: systemUser::["+systemUser+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: systemPass::["+systemPass+"]");
			logger.print(MyLogger.DEBUG, "HDFSFileCopier:: systemHost::["+systemHost+"]");
		}
	}
	
	public boolean copyFromLocal()
	{
		boolean out = true;
		out = out && HDFSFileCopier.copyFromLocal(hdfsUser,hdfsFqdn,hdfsPort,sourceData,hdfsData);
		out = out && HDFSFileCopier.copyFromLocal(hdfsUser,hdfsFqdn,hdfsPort,sourceServers,hdfsData);
		out = out && HDFSFileCopier.copyFromLocal(hdfsUser,hdfsFqdn,hdfsPort,sourceCalendar,hdfsData);
		out = out && HDFSFileCopier.copyFromLocal(hdfsUser,hdfsFqdn,hdfsPort,sourceWorkinghours,hdfsData);
		return out;			
	}
	
	public static boolean copyFromLocal(String user, String fqdn, String port, String src, String dst)
	{
		MyLogger logger = MyLogger.getInstance();
		if (logger.isDebug()) logger.print("DEBUG", "HDFSFileCopier::copyFromLocal");
		boolean out = false;
		FileSystem fs =  null;
		try
		{
			// Chequeo si existe el origen
			if (!(new File (src).exists())) 
			{
				if (logger.isError()) logger.print(MyLogger.ERROR, "HDFSFileCopier::copyFromLocal Source does not exist:"+src);
				return false;
			}
			// Connect
			System.setProperty("HADOOP_USER_NAME", user);
			Configuration conf = new Configuration();
			fs = FileSystem.get(new URI("hdfs://"+fqdn+":"+port), conf);
			Path srcPath = new Path(src);
			Path dstPath = new Path(dst);
			if (logger.isDebug()) logger.print(MyLogger.DEBUG, "HDFSFileCopier::copyFromLocal connected");
			
			// Chequeo si existe el destino
			if (!(fs.exists(dstPath))) 
			{
				if (logger.isError()) logger.print(MyLogger.ERROR, "HDFSFileCopier::copyFromLocal Destination does not exist:"+dst);
				return false;
			}
			
			// Copiado
			fs.copyFromLocalFile(srcPath, dstPath);
			if (logger.isDebug()) logger.print(MyLogger.DEBUG, "HDFSFileCopier::copyFromLocal copied "+ src + " to " + dst);
			
			// Checkeo que se ha copiado
			
			// Si es directorio quito /
			if (src.endsWith("/"))
			{
			  src = src.substring(0, src.length()-1);
			}
			String copiedFileOrDir = src.substring(src.lastIndexOf('/') + 1, src.length());
			 
			// Si no es directorio se lo pongo
			if (!dst.endsWith("/"))
			{
				dst = dst + "/";
			}
			String check = dst + copiedFileOrDir;
			Path checkPath = new Path(check);
			if (!(fs.exists(checkPath))) 
			{
				out = true;
			}
		}
		catch(Exception e)
		{
         	if (logger.isError()) logger.print(MyLogger.ERROR, "HDFSFileCopier::copyFromLocal  ERROR: failed to copy file " +src + " into "+dst);
         	if (logger.isError()) logger.print(MyLogger.ERROR, "HDFSFileCopier::copyFromLocal  ERROR: "+MyUtils.getStackTrace(e));   
         	out = false;
		}
		finally
		{
			if (fs != null)
			{
				try
				{
					fs.close();
				}
				catch(Exception e)
				{
					if (logger.isError()) logger.print(MyLogger.ERROR, "HDFSFileCopier::copyFromLocal  ERROR: failed closing filesystem connection.");
		         	if (logger.isError()) logger.print(MyLogger.ERROR, "HDFSFileCopier::copyFromLocal  ERROR: "+MyUtils.getStackTrace(e));  
		         	out = false;
				}
			}
		}
		if (logger.isError()) logger.print(MyLogger.INFO, "HDFSFileCopier::copyFromLocal  result: "+out);
		return out;
	}
	
	public boolean copyFromLocalRemotely()
	{
		boolean out = true;
		out = out && HDFSFileCopier.copyFromLocalRemotely(systemUser,systemPass,systemHost,sourceData,hdfsData);
		out = out && HDFSFileCopier.copyFromLocalRemotely(systemUser,systemPass,systemHost,sourceServers,hdfsData);
		out = out && HDFSFileCopier.copyFromLocalRemotely(systemUser,systemPass,systemHost,sourceCalendar,hdfsData);
		out = out && HDFSFileCopier.copyFromLocalRemotely(systemUser,systemPass,systemHost,sourceWorkinghours,hdfsData);
		return out;
	}
	
  public static boolean copyFromLocalRemotely (String user, String pass, String host, String src, String dst)
  {
	  MyLogger logger = MyLogger.getInstance();
	  if (logger.isDebug()) logger.print("DEBUG", "HDFSFileCopier::copyFromLocalRemotely");
	  boolean out = false;
	 
	  // Si es directorio quito /
	  if (src.endsWith("/"))
	  {
		  src = src.substring(0, src.length()-1);
	  }
	  
	  // Connection
	  SSHClient ssh = new SSHClient( user, pass, host);
	  ssh.connect();
	  
	  // Copy
	  String command = "hadoop fs -copyFromLocal "+src+" "+dst; 
	  if (logger.isDebug()) logger.print(MyLogger.DEBUG, "HDFSFileCopier::copyFromLocalRemotely  command: "+command);
	  ssh.executeCommand(command);
	  
	  // Check
	  command = "hadoop fs -ls "+dst;
	  if (logger.isDebug()) logger.print(MyLogger.DEBUG, "HDFSFileCopier::copyFromLocalRemotely  command: "+command);
	  List<String> returned = ssh.executeCommand(command);
	  
	  String copiedFileOrDir = src.substring(src.lastIndexOf('/') + 1, src.length());	 
	  for(int i = 0; i < returned.size();i++)
	  {
		  if (returned.get(i).contains(copiedFileOrDir))
		  {
			  out = true;
			  break;
		  }
	  }
	  
	  ssh.closeConnection();
	  if (logger.isError()) logger.print(MyLogger.INFO, "HDFSFileCopier::copyFromLocalRemotely  result: "+out);
	  return out;
  }
  
  private boolean isSourceDataLocal()
  {
	  boolean out = false;
	  if (sourcesTypeLocal.equals(sourceType))
	  {
		  out = true;
	  }
	  return out;
  }
  private boolean isSourceDataRemote()
  {
	  boolean out = false;
	  if (sourcesTypeRemote.equals(sourceType))
	  {
		  out = true;
	  }
	  return out;
	  
  }
	public static boolean load()
	{
		boolean out = false;
		MyLogger logger = MyLogger.getInstance();
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HDFSFileCopier::load");

		String config = "\\conf\\usermodel.conf";
		HDFSFileCopier hdfsfc = new HDFSFileCopier(config);
		
		if (hdfsfc.isSourceDataLocal())
		{
			out = hdfsfc.copyFromLocal();
		}
		else if (hdfsfc.isSourceDataRemote())
		{
			
			out = hdfsfc.copyFromLocalRemotely();
		}
		else
		{
			out = false;
		}
		
		if (logger.isError()) logger.print(MyLogger.INFO, "HDFSFileCopier::load  result: "+out);
		return out;
	}
  
  public static void main (String[] args)
	{
		MyLogger logger = MyLogger.getInstance();
		logger.setLogFileName(System.getProperty("user.dir") + "\\HDFSFileCopier.log");
		logger.setLogLevel(MyLogger.DEBUG);
		logger.open();
		
		boolean remote = false;
		if (remote)
		{
			String user = "root";
			String pass = "s21.sec!";
			String host = "192.168.1.51";
			String source = "/extra/newDir";
			String dest = "/user/root/";
		
			boolean copied = HDFSFileCopier.copyFromLocalRemotely(user,pass,host,source,dest);
			System.out.println("COPIED:"+copied);
		}
		else
		{
			String user = "root";
			String fqdn = "hadoopnode01.osbs.com";
			String port = "8020";
			String source = "D:\\RemoteDir";
			String dest = "/user/root/";

			boolean copied = HDFSFileCopier.copyFromLocal(user,fqdn,port,source,dest);
			System.out.println("COPIED:"+copied);
		}
		logger.close();
	}
 
}
