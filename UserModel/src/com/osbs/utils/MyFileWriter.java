package com.osbs.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class MyFileWriter 
{
	String sFile = null;
	File oFile = null;
	BufferedWriter bw = null;
	int iWritedSize = 0; 
	
	public MyFileWriter(String _sFile)
	{
		sFile = _sFile;

	}
	
	public void openFile()
	{
		this.openFile(true);
	}
	public void openFile(boolean append)
	{
		oFile = new File(sFile); 
		File parentDir = oFile.getParentFile();
		parentDir.mkdirs();
		
		try
		{
			bw = new BufferedWriter(new FileWriter(oFile, append));
		}
		catch (Exception e) 
	    {
			if (MyLogger.getInstance().isError()) MyLogger.getInstance().print("ERROR", "ERROR: creating buffer for file: " +sFile);
			if (MyLogger.getInstance().isError()) MyLogger.getInstance().print("ERROR", "ERROR: "+ MyUtils.getStackTrace(e));
        }
	}
	
	public void writeLine (String sLine)
	{
		if (MyLogger.getInstance().isTrace()) MyLogger.getInstance().print(MyLogger.TRACE, "MyFileWriter::writeLine");
		try
		{ 
			bw.write(sLine+"\n");
			iWritedSize += sLine.length();
			//bw.flush();
		}
	    catch (Exception e) 
	    {
	    	if (MyLogger.getInstance().isError()) MyLogger.getInstance().print("ERROR", "ERROR: writing text: " +sLine+ " in file: " +sFile);
	    	if (MyLogger.getInstance().isError()) MyLogger.getInstance().print("ERROR", "ERROR: "+ MyUtils.getStackTrace(e));
        }
	}
	public void flush ()
	{
		if (MyLogger.getInstance().isTrace()) MyLogger.getInstance().print(MyLogger.TRACE, "MyFileWriter::flush");
		try
		{ 

			bw.flush();
		}
	    catch (Exception e) 
	    {
	    	if (MyLogger.getInstance().isError()) MyLogger.getInstance().print("ERROR", "ERROR: flushing file: " +sFile);
	    	if (MyLogger.getInstance().isError()) MyLogger.getInstance().print("ERROR", "ERROR: "+ MyUtils.getStackTrace(e));
        }
	}
	public void closeFile ()
	{
		if (MyLogger.getInstance().isDebug()) MyLogger.getInstance().print(MyLogger.DEBUG, "MyFileWriter::closeFile");
		try
		{
			bw.flush();
			bw.close();
		}
	    catch (Exception e) 
	    {
	    	if (MyLogger.getInstance().isError()) MyLogger.getInstance().print("ERROR", "ERROR: clossing file: " +sFile);
	    	if (MyLogger.getInstance().isError()) MyLogger.getInstance().print("ERROR", "ERROR: "+ MyUtils.getStackTrace(e));
        }
	}
	
	// Para debug
	public static void main(String[] args) throws Exception
	{
		MyLogger logger = MyLogger.getInstance();
		logger.setLogFileName(System.getProperty("user.dir") + "\\MyFileWriter.log");
		logger.setLogLevel(MyLogger.DEBUG);
		logger.open();
		
		String sFile = "\\\\172.21.1.8\\vol\\vol_testing\\myfile.txt";
		MyFileWriter mfw = new MyFileWriter(sFile);
		mfw.openFile();
		mfw.writeLine("It works!!");
		mfw.closeFile();
		logger.close();
	}



		
}
