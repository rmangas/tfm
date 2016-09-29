package com.osbs.utils;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;

public class MyUtils
{
    public static String getStackTrace(Throwable t)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        t.printStackTrace(pw);
        pw.flush();
        sw.flush();
        return sw.toString();
    }
    
    public static void copyFile(String src, String dst)
    {
						
		// Creamos directorios del de salida si no existen.
		File nf = new File(dst);
		File parentDir = nf.getParentFile();
		parentDir.mkdirs();
	
		// Copiamos
		try
		{
			Files.copy(new File(src).toPath(), nf.toPath(), REPLACE_EXISTING);
		}
		catch (Exception e)
		{
			System.out.println("Error copying file:: "+new File(src).toPath() + " into "+nf.toPath() );
		}
    }
    
    public static void moveFile(String src, String dst)
    {
    	
    	// Creamos directorios del de salida si no existen.
		File nf = new File(dst);
		File parentDir = nf.getParentFile();
		parentDir.mkdirs();
	
		// Movemos
		try
		{
			Files.move(new File(src).toPath(), nf.toPath(), REPLACE_EXISTING);
		}
		catch (Exception e)
		{
			System.out.println("Error moving file:: "+new File(src).toPath() + " into "+nf.toPath() );
		}
    }
}
