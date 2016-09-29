package com.osbs.usermodel.modelbuilder;

import java.util.ArrayList;

import com.osbs.usermodel.tools.HiveConnector;
import com.osbs.usermodel.tools.LoadConfigurations;
import com.osbs.utils.MyLogger;

public class HiveExtractor 
{
	HiveConnector hc = null;
	MyLogger logger = MyLogger.getInstance();
	String serdesLib = null;
	String totalDataFile = null;
	String trainDataFile = null;
	String testDataFile = null;
	double testDataPercent = 10;
	String predictDataFile = null;
	double predictDataPercent = 0.01;
	String database = null;
	
	private HiveExtractor(String usermodel, String extraction)
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor");
		
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.usermodelConfigType, usermodel);
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.extractionConfigType, extraction);
		
		serdesLib = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "serdesLib");
		database =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.database");
		
		totalDataFile = LoadConfigurations.getInstance().getProperty(LoadConfigurations.extractionConfigType, "extraction.total.data.file");
		trainDataFile =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.extractionConfigType, "extraction.training.data.file");
		testDataFile =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.extractionConfigType, "extraction.testing.data.file");
		testDataPercent =  Double.parseDouble(LoadConfigurations.getInstance().getProperty(LoadConfigurations.extractionConfigType, "extraction.testing.data.percentage"));
		predictDataFile =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.extractionConfigType, "extraction.prediction.data.file");
		predictDataPercent =  Double.parseDouble(LoadConfigurations.getInstance().getProperty(LoadConfigurations.extractionConfigType, "extraction.prediction.data.percentage"));
		
		hc = new HiveConnector(usermodel);
		
		if (logger.isDebug())
		{
			logger.print(MyLogger.DEBUG, "HiveExtractor:: serdesLib::["+serdesLib+"]");
			logger.print(MyLogger.DEBUG, "HiveExtractor:: database::["+database+"]");
			logger.print(MyLogger.DEBUG, "HiveExtractor:: totalDataFile::["+totalDataFile+"]");
			logger.print(MyLogger.DEBUG, "HiveExtractor:: trainDataFile::["+trainDataFile+"]");
			logger.print(MyLogger.DEBUG, "HiveExtractor:: testDataFile::["+testDataFile+"]");
			logger.print(MyLogger.DEBUG, "HiveExtractor:: testDataPercent::["+testDataPercent+"]");
			logger.print(MyLogger.DEBUG, "HiveExtractor:: predictDataFile::["+predictDataFile+"]");
			logger.print(MyLogger.DEBUG, "HiveExtractor:: predictDataPercent::["+predictDataPercent+"]");
		}
	}
	
	private boolean connect() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor::connect");
		return hc.connect();
	}
	
	private boolean checkConnection() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor::checkConnection");
		return hc.checkConnection();
	}
	
	private boolean executeInitialCommands() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor::executeInitialCommands");
		ArrayList<String> cl = new ArrayList<String>();
		cl.add("add jar "+serdesLib+"csv-serde-1.1.2-0.11.0-all.jar");
		cl.add("add jar "+serdesLib+"hive-contrib-0.12.0.2.0.10.0-1.jar");
		cl.add("SET hive.auto.convert.join = false");
		cl.add("use "+ database);
		
		return hc.executeCommands(cl);
	}
	
	private boolean loadServers() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor::loadServers");
		String query = "SELECT DISTINCT server FROM SERVERS"; 
		return hc.loadServers(query);
	}

	private boolean extract() throws Exception
	{
		boolean out = false; 
		
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor::extract");
		String query = "SELECT year, month, day, weekday, hour, isFestive, isPreviousFestive, isWeekend, isFriday, isReduced, isWorkingHour, server, connections FROM aggregated_info";

		int rows = hc.extractDataInWekaFormat(query, totalDataFile, trainDataFile, testDataFile, testDataPercent, predictDataFile, predictDataPercent );
		if (rows != -1)
		{
		 out = true;
		}
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor::extract ["+out+"]");
		return out;
	}
	
	
	private boolean close() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor::close");
		return hc.close();
	}
	
	public static boolean launchExtraction() throws Exception
	{
		boolean out = false;
		MyLogger logger = MyLogger.getInstance();
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveExtractor::launchExtraction");
		
		String usermodel = "\\conf\\usermodel.conf";
		String extraction = "\\conf\\extraction.conf";
		
		HiveExtractor ne = new HiveExtractor(usermodel, extraction);
		ne.connect();
		ne.checkConnection();
		ne.executeInitialCommands();
		ne.loadServers();
		out = ne.extract();
		ne.close();
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveExtractor:: Extraction ["+out+"]");
		return out;
		
	}

}
