package com.osbs.utils;

import java.io.BufferedReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class SSHClient
{
	String userName = null;
	String password = null;
	String host = null;
	Connection con = null;
	MyLogger logger = MyLogger.getInstance();

	public SSHClient (String _userName, String _password, String _host)
	{
		userName = _userName;
		password = _password;
		host = _host;
		if (logger.isDebug())
		{
			logger.print(MyLogger.DEBUG, "SSHClient:: userName::["+userName+"]");
			logger.print(MyLogger.DEBUG, "SSHClient:: password::["+password+"]");
			logger.print(MyLogger.DEBUG, "SSHClient:: host::["+host+"]");
		}
	}
	public void connect()
	{
		if (logger.isDebug()) logger.print("DEBUG", "SSHClient::connect");
        try
        {
        	con = new Connection(host);
            con.connect();
            con.authenticateWithPassword(userName, password);
        } 
        catch (Exception e) 
        {
        	if (logger.isError()) logger.print(MyLogger.ERROR, "SSHClient::connect  ERROR: failed to connect "+userName+"@"+ host);
        	if (logger.isError()) logger.print(MyLogger.ERROR, "SSHClient::connect  ERROR: "+MyUtils.getStackTrace(e));   
            if (con != null)
            {
            	con.close();
            }
        }
	}

    public List<String> executeCommand(String command)
    {
    	if (logger.isDebug()) logger.print("DEBUG", "SSHClient::executeCommand:" +command);
    	
    	 List<String> result = new LinkedList<>();
         Session session = null;

         try
         {
             session = con.openSession();
             session.execCommand(command);
             InputStream stdout = new StreamGobbler(session.getStdout());
             
             BufferedReader br = null;
             try 
             {
            	 br = new BufferedReader(new InputStreamReader(stdout));
                 String line = br.readLine();
                 while (line != null) 
                 {
                     result.add(line);
                     line = br.readLine();
                 }
             }
             catch (Exception e)
             {
            	 if (logger.isError()) logger.print(MyLogger.ERROR, "SSHClient::executeCommand  ERROR: failed to retrieve output from execution");
            	 if (logger.isError()) logger.print(MyLogger.ERROR, "SSHClient::executeCommand  ERROR: "+MyUtils.getStackTrace(e));   
             }
             finally 
             {
            	 if (br != null)
                 {
                	 br.close();
                 }
             }
         } 
         catch (Exception e)
         {
          	 if (logger.isError()) logger.print(MyLogger.ERROR, "SSHClient::executeCommand  ERROR: failed to execute command " +command);
          	 if (logger.isError()) logger.print(MyLogger.ERROR, "SSHClient::executeCommand  ERROR: "+MyUtils.getStackTrace(e));   
         }
         finally 
         {
             if (session != null) 
             {
                 session.close(); 
             }
         }
         if (logger.isError()) logger.print(MyLogger.INFO, "SSHClient::executeCommand  result: "+result);
         return result;
   
    }
	public void closeConnection()
	{
		if (logger.isDebug()) logger.print("DEBUG", "SSHClient::closeConnection");
		
		if (con != null)
        {
        	con.close();
        }
	}
	
	public static void main (String[] args)
	{
		MyLogger logger = MyLogger.getInstance();
		logger.setLogFileName(System.getProperty("user.dir") + "\\SSHClient.log");
		logger.setLogLevel(MyLogger.DEBUG);
		logger.open();
		
		String host = "192.168.1.51";
		String user = "root";
		String pass = "s21.sec!";
		
		SSHClient ssh = new SSHClient(user,pass,host);
		ssh.connect();
		
		String command = "ls -ltr /extra";
		List<String> output = ssh.executeCommand(command);
		
		for(int i = 0; i < output.size();i++)
		{
			System.out.println(output.get(i));
		}
		
		ssh.closeConnection();
		
		logger.close();
	}
}
