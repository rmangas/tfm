package com.osbs.utils;

import java.util.ArrayList;
import java.util.List;

public class MyLogger 
{
 
	private static MyLogger singleton = null; // Singleton
	
	static List<String> logLevels = null; // Posibles valores de los niveles de log 
	static String logFileName = "system.log"; // Nombre de fichero de logs del propio 
	static String logLevel = "INFO"; // Nivel del fichero de log por defecto
	static MyFileWriter logFile = null; // File de log
	
	public static final String TRACE = "TRACE";  // Variable estatica con el valor de posible nivel de debug. TRACE
	public static final String DEBUG = "DEBUG";  // Variable estatica con el valor de posible nivel de debug. DEBUG
	public static final String INFO = "INFO";  // Variable estatica con el valor de posible nivel de debug.
	public static final String WARNING = "WARNING";  // Variable estatica con el valor de posible nivel de debug.
	public static final String ERROR = "ERROR";  // Variable estatica con el valor de posible nivel de debug.

	/*
	 * Constructor de logger
	 */
	private MyLogger()
	{
		logLevels = new ArrayList<String>();
		logLevels.add(TRACE);
		logLevels.add(DEBUG);
		logLevels.add(INFO);
		logLevels.add(WARNING);
		logLevels.add(ERROR);
	}
	
	/*
	 * Metodo getInstance del Singleton
	 */
	public static MyLogger getInstance()
	{
		if (singleton == null)
		{
			singleton = new MyLogger();
		}
		return singleton;
	}
	
	// Abre el fichero de log
	public synchronized void open()
	{
		try
		{
			logFile = new MyFileWriter(logFileName);
			System.out.println("logFileName:"+logFileName);
		}
		catch (Exception e)
		{
			System.out.println("No se ha podido crear el fichero de log");
		}
		
		logFile.openFile();
	
	}
	
	/*
	 * Metodo que hace flush del fichero de log
	 */
	public synchronized void flush()
	{
		if (logFile != null)
		{
			try
			{
				logFile.bw.flush();
			}
			catch (Exception e)
			{
				// Nothing
			}
		}
	}
	
	/*
	 * Metodo que cierre el fichero de log
	 */
	public synchronized void close()
	{
		if (logFile != null)
		{
			try
			{
				logFile.closeFile();
			}
			catch (Exception e)
			{
				// Nothing
			}
		}
	}
	
	/*
	 * Metodo para generación de logs
	 */
	public synchronized void print(String level, String msg)
	{
		if(logLevels.indexOf(level) >= logLevels.indexOf(logLevel))
		{
			logFile.writeLine(MyCalendar.getActualTime() + " - " + level + " - "+ msg);
			logFile.flush();
		}
	}

	// GETTERS Y SETTERS
	public String getLogFileName() 
	{
		return logFileName;
	}


	public synchronized void setLogFileName(String logFileName) 
	{
		MyLogger.logFileName = logFileName;
	}


	public String getLogLevel() 
	{
		return logLevel;
	}

	/*
	 * Metodo que comprueba y setea el nivel de log
	 */
	public synchronized void setLogLevel(String level) 
	{
		if (!exists(level))
		{
			//System.out.println("Nivel de log desconocido: " + level + "\n Posibles valores: "+logLevels.toString());
			print("ERROR", "Nivel de log desconocido: " + level);
			print("ERROR", "Se utilizara nivel de log por defecto: " + logLevel);
		}
		else
		{
			MyLogger.logLevel = level;
		}

	}
	
	/*
	 * Metodo qeu comprueba si existe el nivel de log proporcionado
	 */
	public static synchronized boolean exists(String level)
	{
		return logLevels.contains(level);
	}
	/*
	 * Metodo para comprobar el nivel 
	 */
	public boolean hasLevel(String level) 
	{
		boolean bOut = false;
		if(logLevels.indexOf(level) >= logLevels.indexOf(logLevel))
		{
			bOut = true;
		}
		return bOut;
	}

	/*
	 * Metodo para comprobar el nivel 
	 */
	public boolean isTrace() 
	{
		return hasLevel(TRACE);
	}
	
	/*
	 * Metodo para comprobar el nivel 
	 */
	public boolean isDebug() 
	{
		return hasLevel(DEBUG);
	}
	
	/*
	 * Metodo para comprobar el nivel 
	 */
	public boolean isInfo() 
	{
		return hasLevel(INFO);
	}
	
	/*
	 * Metodo para comprobar el nivel 
	 */
	public boolean isWarning() 
	{
		return hasLevel(WARNING);
	}
	
	/*
	 * Metodo para comprobar el nivel 
	 */
	public boolean isError() 
	{
		return hasLevel(ERROR);
	}
	
	
	
}
