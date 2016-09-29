package com.osbs.utils;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class MyFileReader 
{
	String sFile = null;
	File oFile = null;
	BufferedReader br = null;
	int iWritedSize = 0; 
	
	MyLogger logger = null; // Logger
	
	public MyFileReader(String _sFile)
	{
		sFile = _sFile;
		logger = MyLogger.getInstance();
		
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "MyFileReader");
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "MyFileReader sFile:"+sFile);
	}
	
	public void openFile()
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "MyFileReader::openFile");
		
		oFile = new File(sFile); 
		
		try
		{
			br = new BufferedReader(new FileReader(oFile));
		}
		catch (Exception e) 
	    {
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "MyFileReader::openFile ERROR: creating buffer for file: " +sFile);
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "MyFileReader::openFile "+MyUtils.getStackTrace(e));
        }
	}
	
	public String readLine()
	{
		if (MyLogger.getInstance().isTrace()) logger.print(MyLogger.TRACE, "MyFileReader::readLine");
		String sLine = "";
		try
		{ 
			sLine = br.readLine();
		}
	    catch (Exception e) 
	    {
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "MyFileReader::readLine ERROR:reading line in file: " +sFile);
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "MyFileReader::readLine "+MyUtils.getStackTrace(e));
        }
		if (MyLogger.getInstance().isTrace()) logger.print(MyLogger.TRACE, "MyFileReader::readLine: sLine:"+sLine);
		return sLine;
	}
	public void closeFile ()
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "MyFileReader::closeFile");
		try
		{
			br.close();
		}
	    catch (Exception e) 
	    {
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "MyFileReader::closeFile ERROR:closing file: " +sFile);
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "MyFileReader::closeFile "+MyUtils.getStackTrace(e));
        }
	}
	
	// Para debug
	public static void main(String[] args) throws Exception
	{
		MyLogger logger = MyLogger.getInstance();
		logger.setLogFileName(System.getProperty("user.dir") + "\\MyFileReader.log");
		logger.setLogLevel(MyLogger.DEBUG);
		logger.open();
		
		String sFile = "\\\\172.21.1.8\\vol\\vol_testing\\myfile.txt";
		MyFileReader mfr = new MyFileReader(sFile);
		mfr.openFile();
		System.out.println(mfr.readLine());
		mfr.closeFile();
		logger.close();
	}
}
