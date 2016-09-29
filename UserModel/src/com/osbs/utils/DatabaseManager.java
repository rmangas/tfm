package com.osbs.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DatabaseManager 
{

	// Datos de conexion a BBDD
	private String driver; 
	private String url;
	private String user;
	private String pass;
	
	// Conexion a BBDD
	private Connection conn = null;
	
	MyLogger logger = null; // Logger
	
    public DatabaseManager() 
	{
    	logger = MyLogger.getInstance();
	}
	
    public DatabaseManager(String _driver, String _url, String _user, String _pass) 
	{
    	super();
    	logger = MyLogger.getInstance();
    	driver = _driver;
    	url = _url;
    	user = _user;
    	pass = _pass;
    	
    	if (MyLogger.getInstance().isDebug())
		{
			logger.print(MyLogger.DEBUG, "HiveConnector:: logger::["+logger+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: driver::["+driver+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: url::["+url+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: user::["+user+"]");
			logger.print(MyLogger.DEBUG, "HiveConnector:: pass::["+pass+"]");
		}
    }
	
    public Connection getConnection()
	{
    	if (logger.isDebug()) logger.print("DEBUG", "DatabaseManager::getConnection");
    	
		if (conn == null)
		{
			try 
    		{
    	        Class.forName(driver).newInstance();
    	        if (logger.isInfo()) logger.print("INFO", "DatabaseManager::getConnection  Driver: " +driver+ " loaded.");
    	    } 
    		catch (Exception e) 
    	    {
    			if (logger.isError()) logger.print("ERROR", "DatabaseManager::getConnection  ERROR: failed to load "+ driver +" driver.");
    			if (logger.isError()) logger.print("ERROR", "DatabaseManager::getConnection  ERROR: error: "+ MyUtils.getStackTrace(e));
    	    }
    	    try 
    	    {
    	    	conn = DriverManager.getConnection(url, user, pass);
    	    	if (MyLogger.getInstance().isInfo()) logger.print("INFO", "DatabaseManager::getConnection  Connected to: url: " +url+ " user: " +user+ " password:" + pass);
    	    } 
    	    catch (Exception e) 
    	    {
    	    	if (logger.isError()) logger.print("ERROR", "DatabaseManager::getConnection  ERROR: getting connection: url: " +url+ " user: " +user+ " password:" + pass );
    	    	if (logger.isError()) logger.print("ERROR", "DatabaseManager::getConnection  ERROR: error: "+ MyUtils.getStackTrace(e));
    	    }
		}
	  	if (logger.isInfo()) logger.print("INFO", "DatabaseManager::getConnection:[" +(null != conn)+"]");
		return conn;
	}

    public ResultSet executeQuery(String q)
	{
    	if (logger.isDebug()) logger.print("DEBUG", "DatabaseManager::executeQuery");
		
		Statement createStatement = null;
		ResultSet rs = null;
		try 
		{
			createStatement = conn.createStatement();
			rs = createStatement.executeQuery(q);
		} 
		catch (Exception e) 
		{
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::executeQuery:: ERROR: executing query: "+ q);
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::executeQuery:: ERROR: error: "+ MyUtils.getStackTrace(e));
	    }
		if (logger.isInfo()) logger.print("INFO", "DatabaseManager::executeQuery:[" +(null != rs)+"]");
		return rs;
	} 
 
    public ResultSet executeQueryPrepareStatement(String prepareStatement, List<String> prepareValues)
	{
    	if (logger.isDebug()) logger.print("DEBUG", "DatabaseManager::executeQueryPrepareStatement");
		
    	PreparedStatement oPreparedStatement = null;
    	ResultSet rs = null;
		try 
		{
			oPreparedStatement = conn.prepareStatement(prepareStatement);
			for(int i=1; i<= prepareValues.size(); i++ )
			{
				oPreparedStatement.setString(i,prepareValues.get(i-1)); 
			}
			
			rs = oPreparedStatement.executeQuery();
		} 
		catch (Exception e) 
		{
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::executeQueryPrepareStatement:: ERROR: executing query prepared statement: "+ prepareStatement);
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::executeQueryPrepareStatement:: ERROR: executing prepared values: "+ prepareValues);
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::executeQueryPrepareStatement:: ERROR: error: "+ MyUtils.getStackTrace(e));
	    }
		if (logger.isInfo()) logger.print("INFO", "DatabaseManager::executeQueryPrepareStatement:[" +(null != rs)+"]");
		return rs;
	} 
	
    public boolean execute(String q)
	{
    	if (logger.isDebug()) logger.print("DEBUG", "DatabaseManager::execute");
		
		Statement createStatement = null;
		boolean ok = false;
		try 
		{
			createStatement = conn.createStatement();
			ok = createStatement.execute(q);
			ok = true;
		} 
		catch (Exception e) 
		{
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::execute:: ERROR: executing: "+ q);
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::execute:: ERROR: error: "+ MyUtils.getStackTrace(e));
			ok = false;
	    }
		if (logger.isInfo()) logger.print("INFO", "DatabaseManager::execute:[" +(ok)+"]");
		return ok;
	} 
    
    public boolean executePrepareStatement(String prepareStatement, List<String> prepareValues)
	{
    	if (logger.isDebug()) logger.print("DEBUG", "DatabaseManager::executePrepareStatement");
		
    	PreparedStatement oPreparedStatement = null;
    	int returned = -1;
		boolean ok = false;
		try 
		{
			oPreparedStatement = conn.prepareStatement(prepareStatement);
			for(int i=1; i<= prepareValues.size(); i++ )
			{
				oPreparedStatement.setString(i,prepareValues.get(i-1)); 
			}
			
			returned = oPreparedStatement.executeUpdate();
			
			if (returned != 1)
			{
				ok = true;
			}
		} 
		catch (Exception e) 
		{
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::executePrepareStatement:: ERROR: executing prepared statement: "+ prepareStatement);
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::executePrepareStatement:: ERROR: executing prepared values: "+ prepareValues);
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::executePrepareStatement:: ERROR: error: "+ MyUtils.getStackTrace(e));
	    }
		if (logger.isInfo()) logger.print("INFO", "DatabaseManager::executePrepareStatement:[" +(ok)+"]");
		return ok;
	} 
    
    public boolean close()
	{
    	if (logger.isDebug()) logger.print("DEBUG", "DatabaseManager::close");
		
    	boolean ok = false;
		try 
		{
			if (conn != null)
			{
				conn.close();
				conn = null;
			}
		}
		catch (SQLException e) 
		{
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::close:: ERROR: clossing connection");
			if (logger.isError()) logger.print("ERROR", "DatabaseManager::close:: ERROR: error: "+ MyUtils.getStackTrace(e));
		}
		ok = (null == conn);
		if (logger.isInfo()) logger.print("INFO", "DatabaseManager::close:[" +ok+"]");
		return ok;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

}
