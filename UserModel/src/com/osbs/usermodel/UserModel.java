package com.osbs.usermodel;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import com.osbs.usermodel.hadoop.tools.HDFSFileCopier;
import com.osbs.usermodel.modelbuilder.HiveExtractor;
import com.osbs.usermodel.modelbuilder.HiveLoader;
import com.osbs.usermodel.modelbuilder.WekaModelBuilder;
import com.osbs.usermodel.modelbuilder.WekaModelImprover;
import com.osbs.usermodel.predictor.WekaModelPredictor;
import com.osbs.usermodel.startup.HiveModelBuilder;
import com.osbs.utils.MyLogger;
import com.osbs.utils.MyUtils;

public class UserModel
{
	public static void main(String[] args)
	{
		MyLogger logger = MyLogger.getInstance();
		logger.setLogFileName(System.getProperty("user.dir") + "\\UserModel.log");
		logger.setLogLevel(MyLogger.INFO);
		logger.open();
		if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: *** Starting UserModel...");
		
		String[] possibleActions = {"initialDeploy", "loadDataToHDFS", "loadDataToHive", "extractData", "trainingNewClassifier", "classify", "improveExistingClassifier" };
		
		try
		{
			if (args.length != 2 && args.length != 3)
			{	
				String msg = UserModel.getOptionsError();
				UserModel.errorAndExit(msg, true);
			}
			
			// Recogemos el nivel de log, comprobamos y lo seteamos.
			if (null != args[0] && MyLogger.exists(args[0]))
			{
				logger.setLogLevel(args[0]);
			}
			else
			{
				String msg = UserModel.getOptionsError();
				UserModel.errorAndExit(msg, true);
			}
			
			List<String> argsList = Arrays.asList(args);
			List<String> possibleActionsList = Arrays.asList(possibleActions);
			
			// Checkeamos que las opciones estan dentro de las posibles
			for (int argsIndex = 1; argsIndex < argsList.size(); argsIndex++ )
			{
				if (!possibleActionsList.contains(argsList.get(argsIndex)))
				{
					String msg = UserModel.getOptionsError();
					UserModel.errorAndExit(msg,true);
				}
			}
			
			// Tomamos el tiempo inicial
			long initTime = Calendar.getInstance().getTimeInMillis();
			long stepTime = Calendar.getInstance().getTimeInMillis();
			
			// Calculamos acciones inicial y final
			int firstActionIndex = possibleActionsList.indexOf(argsList.get(1));
			int lastActionIndex = firstActionIndex;
			if (args.length == 3) lastActionIndex = possibleActionsList.indexOf(argsList.get(2));
			boolean howAbout = false;
			// Ejecución del deploy inicial
			if (firstActionIndex <= possibleActionsList.indexOf("initialDeploy") &&  possibleActionsList.indexOf("initialDeploy") <=lastActionIndex)
			{
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: Initial Hive Deploy...");
				howAbout = HiveModelBuilder.buildModel();
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel:: Its all OK? "+howAbout);
				if (!howAbout) UserModel.errorAndExit("There was a previous error. Please see log file.",true);
			}
			
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Time taken:: "+(Calendar.getInstance().getTimeInMillis()-stepTime)/(1000*60)+ " minutos.");
			stepTime = Calendar.getInstance().getTimeInMillis();
			
			// Copiado de la información desde las fuentes a HDFS
			if (firstActionIndex <= possibleActionsList.indexOf("loadDataToHDFS") &&  possibleActionsList.indexOf("loadDataToHDFS") <=lastActionIndex)
			{
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: Load Data To HDFS...");
				howAbout = HDFSFileCopier.load();
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel:: Its all OK? "+howAbout);
				if (!howAbout) UserModel.errorAndExit("There was a previous error. Please see log file.",true);
			}
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Time taken:: "+(Calendar.getInstance().getTimeInMillis()-stepTime)/(1000*60)+ " minutos.");
			stepTime = Calendar.getInstance().getTimeInMillis();
			
			//Carga de los datos en Hive
			if (firstActionIndex <= possibleActionsList.indexOf("loadDataToHive") &&  possibleActionsList.indexOf("loadDataToHive") <=lastActionIndex) 
			{
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: Load Data To Hive...");
				howAbout = HiveLoader.load();
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel:: Its all OK? "+howAbout);
				if (!howAbout) UserModel.errorAndExit("There was a previous error. Please see log file.",true);
				
			}
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Time taken:: "+(Calendar.getInstance().getTimeInMillis()-stepTime)/(1000*60)+ " minutos.");
			stepTime = Calendar.getInstance().getTimeInMillis();
			
			// Ejecución de la extraccion de datos de Hive
			if (firstActionIndex <= possibleActionsList.indexOf("extractData") &&  possibleActionsList.indexOf("extractData") <=lastActionIndex) 
			{
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: Extract Data from Hive...");
				howAbout = HiveExtractor.launchExtraction();
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel:: Its all OK? "+howAbout);
				if (!howAbout) UserModel.errorAndExit("There was a previous error. Please see log file.",true);
			}
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Time taken:: "+(Calendar.getInstance().getTimeInMillis()-stepTime)/(1000*60)+ " minutos.");
			stepTime = Calendar.getInstance().getTimeInMillis();
			
			// Ejecución de la creación del clasificador
			if (firstActionIndex <= possibleActionsList.indexOf("trainingNewClassifier") &&  possibleActionsList.indexOf("trainingNewClassifier") <=lastActionIndex) 
			{
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: Training Classifier in Weka...");
				howAbout = WekaModelBuilder.buildModel();
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel:: Its all OK? "+howAbout);
				if (!howAbout) UserModel.errorAndExit("There was a previous error. Please see log file.",true);
				
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Time taken:: "+(Calendar.getInstance().getTimeInMillis()-stepTime)/(1000*60)+ " minutos.");
				stepTime = Calendar.getInstance().getTimeInMillis();
				
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: Evaluate Classifier in Weka...");
				howAbout = WekaModelBuilder.evaluateModel();
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel:: Its all OK? "+howAbout);
				if (!howAbout) UserModel.errorAndExit("There was a previous error. Please see log file.",true);
			} 
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Time taken:: "+(Calendar.getInstance().getTimeInMillis()-stepTime)/(1000*60)+ " minutos.");
			stepTime = Calendar.getInstance().getTimeInMillis();
			
			// Ejecución de la clasificacion de datos
			if (firstActionIndex <= possibleActionsList.indexOf("classify") &&  possibleActionsList.indexOf("classify") <=lastActionIndex)
			{
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: Classify Data in Weka...");
				howAbout = WekaModelPredictor.predict();
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel:: Its all OK? "+howAbout);
				if (!howAbout) UserModel.errorAndExit("There was a previous error. Please see log file.",true);
			}
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Time taken:: "+(Calendar.getInstance().getTimeInMillis()-initTime)/(1000*60)+ " minutos.");
			stepTime = Calendar.getInstance().getTimeInMillis();
			
			// Crea nuevo clasificador y compara con el anterior. Si evaluacion mejor, elimina antiguo y se queda con nuevo
			if (firstActionIndex <= possibleActionsList.indexOf("improveExistingClassifier") &&  possibleActionsList.indexOf("improveExistingClassifier") <=lastActionIndex)
			{
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: Trying to improve model...");
				howAbout = WekaModelImprover.tryToImproveModel();
				if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel:: Its all OK? "+howAbout);
				if (!howAbout) UserModel.errorAndExit("There was a previous error. Please see log file.",true);
			}
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Time taken:: "+(Calendar.getInstance().getTimeInMillis()-stepTime)/(1000*60)+ " minutos.");
			stepTime = Calendar.getInstance().getTimeInMillis();

			// Calculamos tiempo final y consumido y mostramos
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: - Total Time taken:: "+(Calendar.getInstance().getTimeInMillis()-initTime)/(1000*60)+ " minutos.");
			if (logger.isInfo()) logger.print(MyLogger.INFO, "UserModel::main:: *** Closing UserModel.");
			logger.flush();
			logger.close();
		}
		catch (Exception e)
		{
			String msg = "There was a general error. Please see previous logs...";
			UserModel.errorAndExit(msg,false);
			
			msg = MyUtils.getStackTrace(e);
			UserModel.errorAndExit(msg, true);
		}
	}
	
	private static String getOptionsError()
	{
		return "Error in options. Usage::\n  java -jar UserModel.jar <loglevel> <option> [<option>]\nWhere <loglevel> can be ...\n  DEBUG, INFO, WARNING or ERROR.\n<option> can be...\n  initialDeploy: Creates Hive model to insert logs.\n  loadDataToHDFS: Upload logs in HDFS system.\n  loadDataToHive: Load logs in Hive to be analyzed.\n  extractData: Extracts data in Weka format to create model.\n  trainingNewClassifier: Creates a new Weka classifier model.\n  classify: Classifies data.\n  improveExistingClassifier: Try to improve the existing model.\nIf there is only one option, executes that option. If there are two options, all options between these two options are executed in order, both including.";
	}
	private static void errorAndExit(String msg, boolean exit)
	{
		MyLogger logger = MyLogger.getInstance();
		if (logger.isError()) logger.print(MyLogger.ERROR, msg);
		System.out.println(msg);
		logger.flush();
		
		if (exit)
		{
			System.exit(-1);
			logger.close();
		}
	}
}
