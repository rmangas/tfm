package com.osbs.usermodel.predictor;

import com.osbs.usermodel.tools.LoadConfigurations;
import com.osbs.utils.MyFileWriter;
import com.osbs.utils.MyLogger;
import com.osbs.utils.MyUtils;

import weka.classifiers.Classifier;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

public class WekaModelPredictor
{
	//Clasificador
	private Classifier c = null;
	MyLogger logger = MyLogger.getInstance();
	
	String wekaModelFile = null;
	String wekaPredictionInputFile = null;
	String wekaPredictionOutputFile = null;


	private WekaModelPredictor(String prediction)
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelPredictor");
		
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.predictionConfigType, prediction);
		
		wekaModelFile =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.predictionConfigType, "weka.model.file");
		wekaPredictionInputFile =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.predictionConfigType, "weka.prediction.input.file");
		wekaPredictionOutputFile =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.predictionConfigType, "weka.prediction.output.file");
		
		if (logger.isDebug())
		{
			logger.print(MyLogger.DEBUG, "WekaModelPredictor:: wekaModelFile::["+wekaModelFile+"]");
			logger.print(MyLogger.DEBUG, "WekaModelPredictor:: wekaPredictionInputFile::["+wekaPredictionInputFile+"]");
			logger.print(MyLogger.DEBUG, "WekaModelPredictor:: wekaPredictionOutputFile::["+wekaPredictionOutputFile+"]");
		}
	}
	
	public static boolean predict()
	{ 
		String config = "\\conf\\prediction.conf";
		WekaModelPredictor wmp = new WekaModelPredictor(config);
		return wmp.predict(false);
	}
	private boolean predict(boolean showResults)
	{	
		boolean out = false;
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelPredictor::predict");
		try
		{	
			// Deserializamos el modelo y lo cargamos
			c = (Classifier) weka.core.SerializationHelper.read(wekaModelFile);
		
			// Cogemos el fichero y creamos un DataSource con datos a clasificar
			DataSource source = new DataSource(wekaPredictionInputFile);
			
			//Obtenemos las instancias
			Instances data = source.getDataSet();
			
			MyFileWriter mfw = new MyFileWriter(wekaPredictionOutputFile);
			mfw.openFile(false);
			
			// Seteamos la clase 
			 if (data.classIndex() == -1)
			 {
				 data.setClassIndex(data.numAttributes() - 1); 
			 }
			
			// Clasificamos las instancias
			for (int i = 0; i < data.numInstances(); i++) 
		    {
				data.instance(i).setClassMissing();
	            double cls = c.classifyInstance(data.instance(i));
	            long users = Math.round(cls);
	            data.instance(i).setClassValue(users);
				
				String sLine = data.instance(i).toString();
				mfw.writeLine(sLine);
				if (showResults) {System.out.println(sLine);}
		    }
			
			mfw.closeFile();
			out = true;
		}
		catch (Exception e)
		{
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelPredictor::predict::ERROR classifing instances");
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelPredictor::predict::"+MyUtils.getStackTrace(e));
			out = false;
		}
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "WekaModelPredictor::predict:: Classifing:["+out+"]");
		return out;
	}
}
