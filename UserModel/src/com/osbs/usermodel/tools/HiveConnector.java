package com.osbs.usermodel.tools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import com.osbs.utils.DatabaseManager;
import com.osbs.utils.MyFileWriter;
import com.osbs.utils.MyLogger;

public class HiveConnector 
{
	String driver = null;
	String server = null;
	String port = null;
	String database = null;
	String url = null;
	String user = null;
	String pass = null;
	String type = null;
	DatabaseManager dbm = null;
	Connection conn = null;
	
	public static final String noServer = "NO-SERVER";
	List<String> serverList = null;
	
	String[] fields = {"year", "month", "day", "weekday", "hour", "isfestive", "ispreviousfestive", "isweekend", "isfriday", "isreduced", "isworkinghour", "server", "connections"};
	String[] fieldsType = {"{'2014','2015','2016'}", "{'1','2','3','4','5','6','7','8','9','10','11','12'}", "{'1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31'}", "{'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'}", "{'0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'}", "{'true','false'}", "{'true','false'}", "{'true','false'}", "{'true','false'}", "{'true','false'}", "{'true','false'}", "server", "NUMERIC"};

	MyLogger logger = MyLogger.getInstance();
	
// NECESARIO LEVANTAR HIVE EN OTRO PUERTO
//	nohup hive --service hiveserver -p 10001 &

	public HiveConnector (String configFile)
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector");
		
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.usermodelConfigType, configFile);
		
		driver = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.driver");
		server =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.server");
		port =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.port");
		database =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.database");
		user =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.user");
		pass =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.pass");
		
		type = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.type");
		
		if ("1".equals(type))
		{
			driver = "org.apache.hadoop.hive.jdbc.HiveDriver";
			url = "jdbc:hive://"+server+":"+port+"/"+database;
		}
		else if ("2".equals(type))
		{
			driver = "org.apache.hive.jdbc.HiveDriver";
			url = "jdbc:hive2://"+server+":"+port+"/"+database;
		}
		
		if (MyLogger.getInstance().isDebug())
		{
			logger.print(MyLogger.DEBUG, "HiveConnector:: driver::["+driver+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: server::["+server+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: port::["+port+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: database::["+database+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: user::["+user+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: pass::["+pass+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: type::["+type+"]");
		}
		
		serverList = new ArrayList<String>();
	}
	
	public boolean connect() throws SQLException
	{
		boolean out = false;
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::connect");
		
		dbm = new DatabaseManager(driver, url, user, pass);
		conn = dbm.getConnection();
		if (MyLogger.getInstance().isInfo())
		{
			logger.print(MyLogger.INFO, "HiveConnector::connect:: DRIVER:"+driver);
			logger.print(MyLogger.INFO, "HiveConnector::connect:: URL:"+url);
			logger.print(MyLogger.INFO, "HiveConnector::connect:: USER:"+user);
			logger.print(MyLogger.INFO, "HiveConnector::connect:: PASS:"+pass);
			logger.print(MyLogger.INFO, "HiveConnector::connect:: CONNECTED:"+ (null != conn));
		}
		out = (null != conn);
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::connect:: Connect["+out+"]");
		return out;
	}
	
	public boolean checkConnection() throws SQLException
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::checkConnection");
		boolean out = false;
		String q = "show tables";
		ResultSet rs = dbm.executeQuery(q);
		if (rs.next()) 
		{
	       out = true;
	    }
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::checkConnection:: CHECK HIVE CONNECTION::["+out+"]");
		return out;
	}
	
	public boolean existsThisTables(ArrayList<String> tables) throws SQLException
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::existsThisTables:: tables:"+tables);
		boolean out = false;
		String q = "show tables";
		try
		{
			if (null != tables)
			{
				
				ResultSet rs = dbm.executeQuery(q);
				ArrayList<String> returned = new ArrayList<String>();
				
				while (rs.next()) 
				{
					if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::existsThisTables:: table:"+rs.getString(1));
					returned.add(rs.getString(1));
			    }
				
				for (int index = 0 ; index < tables.size(); index++)
				{
					boolean exists = returned.contains(tables.get(index));
					if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::existsThisTables:: exists table:"+rs.getString(1) +" :"+exists);
					if (!exists)
					{
						 out = false;
						 break;
					}
					else
					{
						out = true;
					}
				}
			}
		}
		catch (Exception e)
		{
			if (logger.isError()) logger.print("ERROR", "HiveConnector::existsThisTable:: ERROR: executing query: "+ q);
		}
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::existsThisTable:: existsThisTables::["+out+"]");
		return out;
	}

	public boolean executeCommands(List<String> commands) throws SQLException
	{	
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::executeCommands:: commands::["+commands+"]");
		boolean everythingOK = false;
		if (null != commands)
		{
			everythingOK = true;
			int index = 0;
			while (everythingOK && index < commands.size())
			{
				everythingOK = dbm.execute(commands.get(index));
				if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::executeCommands:: COMMAND ["+ commands.get(index)+ "] EXECUTED:"+everythingOK);
				index++;
			}
		}
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::executeCommands:: COMMANDS::["+everythingOK+"]");
		return everythingOK;
	}
	
	public boolean execute(String query) throws SQLException
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::execute:: query::["+query+"]");
		boolean out = true;
		if (null != query)
		{
			out = dbm.execute(query);
			if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::execute:: query ["+ query+ "] EXECUTED:"+out);
		}
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::execute:: query::["+out+"]");
		return out;
	}
	
	public boolean loadTable (String path, String table)
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::loadTable:: path::["+path+"] + table::["+table+"]");
		boolean out = false;
		if (null != path && null != table)
		{
			String command = "LOAD DATA INPATH '"+path+"' INTO TABLE "+table;
			boolean result = dbm.execute(command);
			out = result;
		}
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::loadTable::"+table+"::["+out+"]");
		return out;
	}
	
	public boolean loadServers(String query) throws SQLException
	{
		boolean out = false;
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::loadServers:: query::["+query+"]");
		ResultSet rs = dbm.executeQuery(query);
		
		if(null != rs)
		{
			while (rs.next())
			{
				serverList.add(rs.getString(1));
				if (MyLogger.getInstance().isTrace()) logger.print(MyLogger.TRACE, "HiveConnector::loadServers:: server::["+rs.getString(1)+"]");
				out = true;
			}
		}
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::loadServers::["+out+"]");
		return out;
	}
		
	public int extractDataInWekaFormat(String sql, String totalDataFile, String trainDataFile, String testDataFile, double testDataPercent, String predictDataFile, double predictDataPercent)  throws SQLException
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::extract:: sql::["+sql+"]");
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::extract:: totalDataFile::["+totalDataFile+"]");
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::extract:: trainDataFile::["+trainDataFile+"]");
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::extract:: testDataFile::["+testDataFile+"]");
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::extract:: testDataPercent::["+testDataPercent+"]");
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::extract:: predictDataFile::["+predictDataFile+"]");
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::extract:: predictDataPercent::["+predictDataPercent+"]");
		
		int totalRows = -1;
		int trainRows = -1;
		int testRows = -1;
		int predictRows = -1;

		long initTime = Calendar.getInstance().getTimeInMillis();
		long endTime = 0;
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::extract:: query::["+sql+"]");

		ResultSet rs = dbm.executeQuery(sql);
		
		if(null != rs)
		{
			// Abrimos todos los ficheros de salida
			MyFileWriter mfwTotalDataFile = new MyFileWriter(totalDataFile);
			mfwTotalDataFile.openFile(false);
			MyFileWriter mfwTrainDataFile = new MyFileWriter(trainDataFile);
			mfwTrainDataFile.openFile(false);
			MyFileWriter mfwTestDataFile = new MyFileWriter(testDataFile);
			mfwTestDataFile.openFile(false);
			MyFileWriter mfwPredictDataFile = new MyFileWriter(predictDataFile);
			mfwPredictDataFile.openFile(false);
			
			// Iniciamos los ficheros
			mfwTotalDataFile.writeLine("%");
			mfwTrainDataFile.writeLine("%");
			mfwTestDataFile.writeLine("%");
			mfwPredictDataFile.writeLine("%");
			mfwTotalDataFile.writeLine("%");
			mfwTrainDataFile.writeLine("%");
			mfwTestDataFile.writeLine("%");
			mfwPredictDataFile.writeLine("%");
			/*
		
			mfwTotalDataFile
			mfwTrainDataFile
			mfwTestDataFile
			mfwPredictDataFile
		*/	
			// Relation
			mfwTotalDataFile.writeLine("@RELATION aggregated_info_total");
			mfwTrainDataFile.writeLine("@RELATION aggregated_info_train");
			mfwTestDataFile.writeLine("@RELATION aggregated_info_test");
			mfwPredictDataFile.writeLine("@RELATION aggregated_info_predict");

			// Atributos
			int colunms = rs.getMetaData().getColumnCount();
			int countTotal =0;
			for (int col = 1 ; col <= colunms; col++)
			{
				String colName = rs.getMetaData().getColumnName(col);
				String type = getWekaColumnType(colName);
				
				mfwTotalDataFile.writeLine("@ATTRIBUTE "+ colName + " " +type);
				mfwTrainDataFile.writeLine("@ATTRIBUTE "+ colName + " " +type);
				mfwTestDataFile.writeLine("@ATTRIBUTE "+ colName + " " +type);
				mfwPredictDataFile.writeLine("@ATTRIBUTE "+ colName + " " +type);
			}

			// DATA
			mfwTotalDataFile.writeLine("@DATA");
			mfwTrainDataFile.writeLine("@DATA");
			mfwTestDataFile.writeLine("@DATA");
			mfwPredictDataFile.writeLine("@DATA");
			
			while (rs.next()) 
			{
				StringBuffer sbLine = new StringBuffer();
				for (int col = 1 ; col <= colunms; col++)
				{
					sbLine.append(rs.getString(col));
					sbLine.append(",");
				}
				String line = sbLine.substring(0, sbLine.length()-1);
		        countTotal++;
		               
		        // Excluyente la escritura menos para total
		        if ( countTotal % (100/predictDataPercent) == 0)
		        {
		        	 // Escribir linea a Fichero Prediccion
		        	mfwPredictDataFile.writeLine(line);
		        	predictRows++;
		        }
		        else if (countTotal % (100/testDataPercent) == 1)
		        {
		        	 // Escribir linea a Fichero Test
		        	mfwTestDataFile.writeLine(line);
		        	testRows++;
		        }
		        else
		        {
		        	 // Escribir linea a Fichero Train
		        	mfwTrainDataFile.writeLine(line);
		        	trainRows++;
		        }
		        
		        // Escribir linea a Fichero Total
		        mfwTotalDataFile.writeLine(line);

		    }
			endTime = Calendar.getInstance().getTimeInMillis();
			
			// Final fichero
			mfwTotalDataFile.writeLine("%");
			mfwTrainDataFile.writeLine("%");
			mfwTestDataFile.writeLine("%");
			mfwPredictDataFile.writeLine("%");
			mfwTotalDataFile.writeLine("%");
			mfwTrainDataFile.writeLine("%");
			mfwTestDataFile.writeLine("%");
			mfwPredictDataFile.writeLine("%");
			
			mfwPredictDataFile.closeFile();
			mfwTestDataFile.closeFile();
			mfwTrainDataFile.closeFile();
			mfwTotalDataFile.closeFile();
			
			totalRows = countTotal;
		}
		else
		{
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "HiveConnector::extract:: There is no rows to evaluate");
		}
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::extract:: TOTAL TIME::["+(endTime-initTime)/(1000*60)+ " minutos.]");
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::extract:: TOTAL ROWS EXTRACTED::["+totalRows+"]");
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::extract:: TRAIN ROWS EXTRACTED::["+trainRows+"]");
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::extract:: TEST ROWS EXTRACTED::["+testRows+"]");
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveConnector::extract:: PREDICT ROWS EXTRACTED::["+predictRows+"]");
		
		return totalRows;
	}
	private String getWekaColumnType(String colName) 
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::getWekaColumnType:: colName::["+colName+"]");
		String type = null;
		if ("server".equals(colName))
		{
			
			List<String> lServers = this.getServerList();
			type = "{'" + noServer;
			for(int i = 0; i < lServers.size(); i++)
			{
				type = type + "','" + lServers.get(i);
			}
			type = type + "'}";
		}
		else
		{
			int index = Arrays.asList(fields).indexOf(colName);
			if (index != -1)
			{
				type = Arrays.asList(fieldsType).get(index);
			}
		}
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::getWekaColumnType:: type::["+type+"]");
		return type;
	}
	
	private List<String> getServerList() 
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::getServerList");
		return serverList;
	}

	public boolean close()
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveConnector::close");
		return dbm.close();
	}

	public static void main( String[] args) throws Exception
	{
		MyLogger logger = MyLogger.getInstance();
		logger.setLogFileName(System.getProperty("user.dir") + "\\"+HiveConnector.class.getName()+".log");
		logger.setLogLevel(MyLogger.DEBUG);
		logger.open();
		
		String ddbbFile = "\\conf\\database.conf";
		HiveConnector hc = new HiveConnector(ddbbFile);
		hc.connect();
		hc.checkConnection();
		
		String sql = "SELECT year, month, day, hour, isFestive, isPreviousFestive, isWeekend, isFriday, isReduced, isWorkingHour, server, connections FROM aggregated_info";
		String total = System.getProperty("user.dir") + "\\hive_total_extraction.arff";
		String train = System.getProperty("user.dir") + "\\hive_train_extraction.arff";
		String test = System.getProperty("user.dir") + "\\hive_test_extraction.arff";
		String predict = System.getProperty("user.dir") + "\\hive_predict_extraction.arff";
		double testP = 5;
		double predictP = 0.0001;
		
		hc.extractDataInWekaFormat(sql, total,train,test,testP, predict, predictP );
		hc.close();
		logger.flush();
		logger.close();
	}
}
