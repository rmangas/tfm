package com.osbs.usermodel.modelbuilder;

import java.util.ArrayList;

import com.osbs.usermodel.tools.HiveConnector;
import com.osbs.usermodel.tools.LoadConfigurations;
import com.osbs.utils.MyLogger;

public class HiveLoader 
{
	HiveConnector hc = null;
	MyLogger logger = MyLogger.getInstance();
	String serdesLib = null;
	String sourceData = null;
	String hdfsData = null;
	String database = null;
	
	private HiveLoader(String usermodel)
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveLoader");
		
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.usermodelConfigType, usermodel);
		serdesLib = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "serdesLib");
		sourceData = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "source.data");
		hdfsData =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "hdfs.data");
		database =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.database");
		
		hc = new HiveConnector(usermodel);
		
		if (logger.isDebug())
		{
			logger.print(MyLogger.DEBUG, "HiveLoader:: serdesLib::["+serdesLib+"]");
			logger.print(MyLogger.DEBUG, "HiveLoader:: sourceData::["+sourceData+"]");
			logger.print(MyLogger.DEBUG, "HiveLoader:: hdfsData::["+hdfsData+"]");
			logger.print(MyLogger.DEBUG, "HiveLoader:: database::["+database+"]");
		}
	}
	
	private boolean connect() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveLoader::connect");
		return hc.connect();
	}
	
	private boolean checkConnection() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveLoader::checkConnection");
		return hc.checkConnection();
	}
	
	private boolean executeInitialCommands() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveLoader::executeInitialCommands");
		ArrayList<String> cl = new ArrayList<String>();
		cl.add("add jar "+serdesLib+"csv-serde-1.1.2-0.11.0-all.jar");
		cl.add("add jar "+serdesLib+"hive-contrib-0.12.0.2.0.10.0-1.jar");
		cl.add("SET hive.auto.convert.join = false");
		cl.add("use "+ database);
		
		boolean out = hc.executeCommands(cl);
		return out;
	}
	
	private boolean loadData() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveLoader::loadData");
		
		String finalDir = sourceData;
		if (sourceData.endsWith("/")) finalDir = sourceData.substring(0, sourceData.length()-1);
		finalDir = finalDir.substring(finalDir.lastIndexOf('/') + 1, finalDir.length());

		ArrayList<String> ld = new ArrayList<String>();
		ld.add("LOAD DATA INPATH '"+hdfsData+finalDir+"' INTO TABLE raw_log");
		ld.add("LOAD DATA INPATH '"+hdfsData+"machines.csv' INTO TABLE SERVERS");
		ld.add("LOAD DATA INPATH '"+hdfsData+"calendar.csv' INTO TABLE CALENDAR");
		ld.add("LOAD DATA INPATH '"+hdfsData+"working.csv' INTO TABLE WORKINGHOURS");
		
		boolean out = hc.executeCommands(ld);
		return out;

	}
	
	
	private boolean close() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveLoader::close");
		return hc.close();
	}
	
	public static boolean load() throws Exception
	{
		MyLogger logger = MyLogger.getInstance();
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveLoader::load");
		String usermodel = "\\conf\\usermodel.conf";
		HiveLoader hl = new HiveLoader(usermodel);
		hl.connect();
		hl.checkConnection();
		hl.executeInitialCommands();
		boolean out = hl.loadData();
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveLoader::load::["+out+"]");
		hl.close();
		return out;
	}

}
