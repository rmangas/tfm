package com.osbs.usermodel.modelbuilder;

import java.io.Serializable;
import java.util.HashMap;

import com.osbs.utils.MyFileReader;

public class WekaTestResults implements Serializable
{

	private static final long serialVersionUID = 1L;
	
	HashMap<String,String> results = null;
	
	public WekaTestResults()
	{
		results = new HashMap<String,String>();
	}
	
	public static WekaTestResults createWekaTestResultsFromFile(String file)
	{
		WekaTestResults wtr = new WekaTestResults();
		MyFileReader mfr = new MyFileReader(file);
		mfr.openFile();
		String line = null;
		while ((line = mfr.readLine()) != null)
		{
			// Linea vacia, Results o ===, pasar siguiente linea
			if (line.trim().length() == 0 || line.trim().equals("Results") || line.trim().startsWith("=="))
			{
				continue;
			}
			// Linea con datos, leer hasta
			else
			{
				String var = "";
				String value = "";
				String[] ss = line.trim().split(" ");
				
				for (int index = 0; index < ss.length; index ++)
				{
					if (ss[index].length() != 0)
					{
						// Si es letra, añado a var
						if (Character.isLetter(ss[index].charAt(0)))
						{
							var = var + ss[index] + " ";
						}
						// Añado a value
						else
						{
							value = ss[index];
							break;
						}
					}
				}
			    wtr.addResult(var.trim(), value);
			}
		}
		mfr.closeFile();
		return wtr;
	}
	
	public void addResult(String resultName, String resultValue)
	{
		results.put(resultName, resultValue);
	}
	public String getResult(String resultName)
	{
		return results.get(resultName);
	}
	public HashMap<String,String> getAllResults()
	{
		return results;
	}
	
	public static int compareWekaResults(WekaTestResults wr1, WekaTestResults wr2, String resultName)
	{
		// -1 si wr1 < wr2, 0 si wr1 = wr2, 1 si rw1 > wr2
		int out = 0;
		
		String val1 = wr1.getResult(resultName);
		String val2 = wr2.getResult(resultName);
		
		double d = (Double.parseDouble(val1) - Double.parseDouble(val2));

		if ( d < 0)
		{
			out = -1;
		}
		else if ( d > 0)
		{
			out = 1;
		}
		else
		{
			out = 0;
		}
		return out;
	}
	// final, inicial, porcentaje
	public static int compareWekaResultsPercent(WekaTestResults wr1, WekaTestResults wr2, String resultName, double increment)
	{
		// -1 si wr1 < wr2*(1+increment), 0 si wr1 = wr2(1+increment), 1 si rw1 > wr2(1+increment)
		int out = 0;
		
		String val1 = wr1.getResult(resultName);
		String val2 = wr2.getResult(resultName);

		double d = Double.parseDouble(val1) - Double.parseDouble(val2)*(1+increment);
		if ( d < 0)
		{
			out = -1;
		}
		else if ( d > 0)
		{
			out = 1;
		}
		else
		{
			out = 0;
		}
		return out;
	}
	
	public static void main (String[] args)
	{
		String file1 = "\\\\192.168.1.40\\home\\TFM\\Data\\WEKA\\t1\\test.out";
		WekaTestResults wtr1 = WekaTestResults.createWekaTestResultsFromFile(file1);
		HashMap<String,String> map1 = wtr1.getAllResults();
		map1.forEach((k,v)->System.out.println("Item : " + k + " Value : " + v));
		
		String file2 = "\\\\192.168.1.40\\home\\TFM\\Data\\WEKA\\t1\\test1.out";
		WekaTestResults wtr2 = WekaTestResults.createWekaTestResultsFromFile(file2);
		HashMap<String,String> map2 = wtr1.getAllResults();
		map2.forEach((k,v)->System.out.println("Item : " + k + " Value : " + v));
		
		String resultName = "Correlation coefficient";
		double increment = 0.05;
		System.out.println(WekaTestResults.compareWekaResultsPercent(wtr1, wtr2, resultName,increment));
	}

}
