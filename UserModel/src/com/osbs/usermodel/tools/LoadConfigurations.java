package com.osbs.usermodel.tools;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.osbs.utils.MyLogger;
import com.osbs.utils.MyUtils;

public class LoadConfigurations 
{
	public static final String usermodelConfigType = "USERMODEL-CONFIG"; 
	public static final String extractionConfigType = "EXTRACTION-CONFIG"; 
	public static final String trainingConfigType = "TRAINING-CONFIG"; 
	public static final String improvingConfigType = "IMPROVING-CONFIG"; 
	public static final String predictionConfigType = "PREDICTION-CONFIG"; 
	
	public Map<String, Properties> configs = null;
	private static LoadConfigurations singleton = null;
	
	MyLogger logger = MyLogger.getInstance();
	
	private LoadConfigurations()
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "LoadConfigurations");
		configs = new HashMap<String, Properties>();
	}
	
	public static LoadConfigurations getInstance()
	{
		MyLogger logger = MyLogger.getInstance();
		if (logger.isDebug()) logger.print(MyLogger.DEBUG, "LoadConfigurations::getInstance");
		
		if (singleton == null)
		{
			singleton = new LoadConfigurations();
		}
		return singleton;
	}
	public void loadConfig(String type, String file)
	{
		if (logger.isDebug()) logger.print(MyLogger.DEBUG, "LoadConfigurations::loadConfig");
		try
		{
			if (!isLoaded(type))
			{
				String currentDir = System.getProperty("user.dir");
				Properties config = new Properties();
				InputStream in =  new FileInputStream(currentDir+file);
				config.load(in);
				configs.put(type, config);
				in.close();
			}
		}
		catch (Exception e)
		{
			if (logger.isError()) logger.print(MyLogger.ERROR, "LoadConfigurations::loadConfig  Error loading config file["+file+"] of type:["+type+"]");
			if (logger.isError()) logger.print(MyLogger.ERROR, "LoadConfigurations::loadConfig  "+MyUtils.getStackTrace(e));
		} 
	}
	
	public boolean isLoaded(String type)
	{
		return configs.containsKey(type);
	}
	
	public String getProperty(String type, String name)
	{
		return configs.get(type).getProperty(name);
	}
}
