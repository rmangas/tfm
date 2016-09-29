package com.osbs.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class MyCalendar 
{
	
	public static final String timestampFormat = "yyyy-MM-dd HH:mm:ss"; // Formato de timestamp
	private static Calendar singleton = null; // Singleton
	
	/*
	 * Metodo getInstance del Singleton
	 */
	public static Calendar getInstance()
	{
		if (singleton == null)
		{
			singleton = Calendar.getInstance();
		}
		return singleton;
	}
	
	/*
	 * Método para obtener la fecha actual en formato necesario
	 */
	public static synchronized String getActualTime() 
	{
		String sOut = "";
		singleton = MyCalendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(timestampFormat);
		sOut = sdf.format(singleton.getTime());
		return sOut;
	}
	
	/*
	 * Método para obtener la fecha actual en formato necesario
	 */
	public static synchronized String getActualTime(String format) 
	{
		String sOut = "";
		singleton = MyCalendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		sOut = sdf.format(singleton.getTime());
		return sOut;
	}
}
