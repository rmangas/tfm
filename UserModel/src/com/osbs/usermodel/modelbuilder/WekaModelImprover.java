package com.osbs.usermodel.modelbuilder;

import com.osbs.usermodel.tools.LoadConfigurations;
import com.osbs.utils.MyCalendar;
import com.osbs.utils.MyLogger;
import com.osbs.utils.MyUtils;

public class WekaModelImprover 
{
	public static final String timestampFormat = "yyyyMMddHHmmss"; // Formato de timestamp
	
	MyLogger logger = MyLogger.getInstance();
	
	String wekaTestResultFile = null;
	String wekaModelFile = null;

	String wekaImprovingTrainDataFile = null;
	String wekaImprovingTestDataFile = null;
	String wekaImprovingTestResultFile = null;
	String wekaImprovingAlgorith = null;
	String wekaImprovingAlgorithOptions = null;
	String wekaImprovingModelFile = null;
	String wekaImproveStatistic = null;
	String wekaImprovePercent = null;
	String wekaImproveMaxIterations = null;

	public WekaModelImprover(String trainConfig, String improvingConfig)
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelImprover");
		
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.trainingConfigType, trainConfig);
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.improvingConfigType, improvingConfig);

		wekaTestResultFile = LoadConfigurations.getInstance().getProperty(LoadConfigurations.trainingConfigType, "weka.test.result.file");
		wekaModelFile = LoadConfigurations.getInstance().getProperty(LoadConfigurations.trainingConfigType, "weka.model.file");
		
		
		wekaImprovingTrainDataFile = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.train.data.file");
		wekaImprovingTestDataFile = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.test.data.file");
		wekaImprovingTestResultFile = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.test.result.file");
		wekaImprovingAlgorithOptions = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.algorith.options");
		wekaImprovingAlgorith = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.algorith");
		wekaImprovingModelFile = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.model.file");
		wekaImproveStatistic = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.improve.statistic");
		wekaImprovePercent = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.improve.percent");
		wekaImproveMaxIterations = LoadConfigurations.getInstance().getProperty(LoadConfigurations.improvingConfigType, "weka.improve.max.iterations");

		if (MyLogger.getInstance().isDebug())
		{
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaTestResultFile::"+wekaTestResultFile);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaModelFile::"+wekaModelFile);
			
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImprovingTrainDataFile::"+wekaImprovingTrainDataFile);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImprovingTestDataFile::"+wekaImprovingTestDataFile);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImprovingTestResultFile::"+wekaImprovingTestResultFile);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImprovingAlgorith::"+wekaImprovingAlgorith);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImprovingAlgorithOptions::"+wekaImprovingAlgorithOptions);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImprovingModelFile::"+wekaImprovingModelFile);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImproveStatistic::"+wekaImproveStatistic);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImprovePercent::"+wekaImprovePercent);
			logger.print(MyLogger.DEBUG, "WekaModelImprover:: wekaImproveMaxIterations::"+wekaImproveMaxIterations);
		}
	}
	
	public static boolean tryToImproveModel() 
	{
		boolean out = false;
		MyLogger logger = MyLogger.getInstance();
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelImprover::tryToImproveModel");
		String configTraining = "\\conf\\training.conf";
		String configImproving = "\\conf\\improving.conf";
		WekaModelImprover wmi  = new WekaModelImprover(configTraining,configImproving);
		
		int it = 0;
		int isBetter = 0;
		while (it < Integer.parseInt(wmi.wekaImproveMaxIterations))
		{
			// Generamos nuevo modelo
			boolean bm = WekaModelBuilder.buildModel(LoadConfigurations.improvingConfigType, configImproving);
			if (!bm) return false;
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::tryToImproveModel:: modelo generado:"+bm);
			
			// Evaluamos nuevo modelo
			boolean ev = WekaModelBuilder.evaluateModel(LoadConfigurations.improvingConfigType,configImproving);
			if (!ev) return false;
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::tryToImproveModel:: modelo evaluado:"+ev);
			
			// Vemos Si es necesario o no reemplazarlo
			// Check if the new one is better 
			WekaTestResults wtrNewOne = WekaTestResults.createWekaTestResultsFromFile(wmi.wekaImprovingTestResultFile);
			WekaTestResults wtrActual = WekaTestResults.createWekaTestResultsFromFile(wmi.wekaTestResultFile);
			isBetter = WekaTestResults.compareWekaResultsPercent(wtrNewOne, wtrActual, wmi.wekaImproveStatistic,Double.parseDouble(wmi.wekaImprovePercent));
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::tryToImproveModel:: Modelo nuevo mejor que actual:"+isBetter);
			
			if (isBetter > 0)
			{
				out =  wmi.replaceModel(); // Hemos mejorado, vamos a guardarlo
				break;
			}
			else
			{
				out = true; // No hemos mejorado pero hemos acabdo bien la vuelta
			}
			it++;
		}
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover:: Try To Improve Model ["+out+"] - Improved? ["+(isBetter>0)+"]");
		return out;
	}
	

	private boolean replaceModel() 
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel");
		boolean out = false;
		try
		{
			//  BKP Modelo viejo y TestResult Viejo, copiandolos con _DATE.BKP
			String src = wekaTestResultFile;
			String dst = wekaTestResultFile+"_"+MyCalendar.getActualTime(timestampFormat)+".bkp";
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: src:"+src);
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: dst:"+dst);
			MyUtils.copyFile(src, dst);
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: test results bkp created");
			
			src = wekaModelFile;
			dst = wekaModelFile+"_"+MyCalendar.getActualTime(timestampFormat)+".bkp";
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: src:"+src);
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: dst:"+dst);
			MyUtils.copyFile(src, dst);
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: model bkp created");
			
			//  Sobreescribir ficheros viejos con nuevos
			src = wekaImprovingTestResultFile;
			dst = wekaTestResultFile;
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: src:"+src);
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: dst:"+dst);
			MyUtils.copyFile(src, dst);
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: test results updated");
			
			src = wekaImprovingModelFile;
			dst = wekaModelFile;
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: src:"+src);
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: dst:"+dst);
			MyUtils.copyFile(src, dst);
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover::replaceModel:: model updated");
			
			out = true;
		}
		catch (Exception e)
		{
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelImprover:: ERROR replacing model");
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelImprover:: "+MyUtils.getStackTrace(e));
			out = false;
		}
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.DEBUG, "WekaModelImprover:: Replace Model ["+out+"]");
		return out;
	}

}
