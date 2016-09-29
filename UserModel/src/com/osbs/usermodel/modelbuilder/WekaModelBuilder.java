package com.osbs.usermodel.modelbuilder;

import java.io.File;
import java.lang.reflect.Constructor;

import com.osbs.usermodel.predictor.WekaModelPredictor;
import com.osbs.usermodel.tools.LoadConfigurations;
import com.osbs.utils.MyFileWriter;
import com.osbs.utils.MyLogger;
import com.osbs.utils.MyUtils;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;


public class WekaModelBuilder 
{

	//Clasificador
	Classifier c = null;
	MyLogger logger = MyLogger.getInstance();
	
	String wekaTrainDataFile = null;
	String wekaTestDataFile = null;
	String wekaTestResultFile = null;
	String wekaModelFile = null;
	String wekaAlgorith = null;
	String wekaAlgorithOptions = null;
	
	private WekaModelBuilder(String configType, String training)
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder");
		
		LoadConfigurations.getInstance().loadConfig(configType, training);
		wekaTrainDataFile = LoadConfigurations.getInstance().getProperty(configType, "weka.train.data.file");
		wekaTestDataFile = LoadConfigurations.getInstance().getProperty(configType, "weka.test.data.file");
		wekaTestResultFile = LoadConfigurations.getInstance().getProperty(configType, "weka.test.result.file");
		wekaModelFile = LoadConfigurations.getInstance().getProperty(configType, "weka.model.file");
		wekaAlgorith = LoadConfigurations.getInstance().getProperty(configType, "weka.algorith");
		wekaAlgorithOptions = LoadConfigurations.getInstance().getProperty(configType, "weka.algorith.options");
		
		if (MyLogger.getInstance().isDebug())
		{
			logger.print(MyLogger.DEBUG, "WekaModelBuilder:: wekaTrainDataFile::"+wekaTrainDataFile);
			logger.print(MyLogger.DEBUG, "WekaModelBuilder:: wekaTestDataFile::"+wekaTestDataFile);
			logger.print(MyLogger.DEBUG, "WekaModelBuilder:: wekaTestResultFile::"+wekaTestResultFile);
			logger.print(MyLogger.DEBUG, "WekaModelBuilder:: wekaModelFile::"+wekaModelFile);
			logger.print(MyLogger.DEBUG, "WekaModelBuilder:: wekaAlgorith::"+wekaAlgorith);
			logger.print(MyLogger.DEBUG, "WekaModelBuilder:: wekaAlgorithOptions::"+wekaAlgorithOptions);
		}
		
		try
		{
			// Creo el objeto de clasificador correspondiente
			Class<?> myClass = Class.forName(wekaAlgorith);
			Constructor<?> cons = myClass.getConstructor();
			c = (Classifier) cons.newInstance();
		}
		catch (Exception e)
		{
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelBuilder:: ERROR building WekaModelBuilder constructor");
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelBuilder:: "+MyUtils.getStackTrace(e));
		}
	}
	private boolean trainModel()
	{
		return this.trainModel(false, false);
	}
	private boolean trainModel(boolean debug, boolean showResults)
	{	
		boolean out = false;
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::trainModel");
		try
		{
			 // Seteamos opciones del clasificador
			 c.setDebug(debug);
			 if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::trainModel:: setDebug");
			 
			 // Seteamos las opciones de entrada
			 c.setOptions(weka.core.Utils.splitOptions(wekaAlgorithOptions));
			 if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::trainModel:: setOptions");
			 
			 // Cogemos el fichero y creamos un DataSource con datos entrenamiento
			 DataSource source = new DataSource(wekaTrainDataFile);
			 if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::trainModel:: DataSource");
			 
			 //Obtenemos las instancias de entrenamiento
			 Instances data = source.getDataSet();
			 if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::trainModel:: getDataSet");
			 
			 // Seteamos la clase
			 if (data.classIndex() == -1)
			 {
				 data.setClassIndex(data.numAttributes() - 1); 
				 if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::trainModel:: setClassIndex");
			 }
			 
			 
			 //Ejecutamos el clasificador
			 c.buildClassifier(data);
			 if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::trainModel:: buildClassifier");
			 
			 if (showResults)
			 {
				System.out.println(c.toString());
			 }
			 out = true;

		}
		catch (Exception e)
		{
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelBuilder::trainModel::ERROR building classifier model");
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelBuilder::trainModel::"+MyUtils.getStackTrace(e));
			out = false;
		}
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "WekaModelBuilder::trainModel:: Training Model:["+out+"]");
		return out;
	}

	private boolean storeModel()
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::storeModel");
		boolean out = false;
		try
		{
			// Creamos los directorios si no existen
			File f = new File(wekaModelFile);
			File parentDir = f.getParentFile();
			parentDir.mkdirs();
			
			 // Serializamos el modelo y lo guardamos			
			 weka.core.SerializationHelper.write(wekaModelFile, c);
			 out = true;
		}
		catch (Exception e)
		{
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelBuilder::storeModel::ERROR storing classifier model");
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelBuilder::storeModel::"+MyUtils.getStackTrace(e));
			out = false;
		}
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "WekaModelBuilder::storeModel:: Storing Model:["+out+"]");
		return out;
	}
	
	private boolean evaluate() 
	{
		return this.evaluate(false);
	}
	private boolean evaluate(boolean debug) 
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::evaluate");
		
		boolean out = false;
		try
		{
			 // Deserializamos el modelo y lo cargamos
			 c = (Classifier) weka.core.SerializationHelper.read(wekaModelFile);

		     // Seteamos opciones del clasificador
			 c.setDebug(debug);
			 
			 // Seteamos las opciones de entrada
			 c.setOptions(weka.core.Utils.splitOptions(wekaAlgorithOptions));
			 
			 // Cogemos el fichero y creamos un DataSource con datos entrenamiento
			 DataSource sourceTrain = new DataSource(wekaTrainDataFile);
			 //Obtenemos las instancias de entrenamiento
			 Instances dataTrain = sourceTrain.getDataSet();
			 
			 // Cogemos el fichero y creamos un DataSource con datos test
			 DataSource sourceTest = new DataSource(wekaTestDataFile);
			 //Obtenemos las instancias de test
			 Instances dataTest = sourceTest.getDataSet();
			 
			 // Seteamos la clase para los datos de entrenamiento
			 if (dataTrain.classIndex() == -1)
			 {
				 dataTrain.setClassIndex(dataTrain.numAttributes() - 1); 
			 }
			 
			 // Seteamos la clase para los datos de test
			 if (dataTest.classIndex() == -1)
			 {
				 dataTest.setClassIndex(dataTest.numAttributes() - 1); 
			 }
			 
			 //Evaluamos el clasificador
			 Evaluation eval = new Evaluation(dataTrain);
			 eval.evaluateModel(c, dataTest);
			 
			 // Guardamos salida a fichero
			 MyFileWriter mfw = new MyFileWriter(wekaTestResultFile);
			 mfw.openFile(false);
			 mfw.writeLine(eval.toSummaryString("\nResults\n======\n", false));
			 mfw.closeFile();
			 out = true;
		}
		catch (Exception e)
		{
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelBuilder::evaluate::ERROR evaluating classifier model");
			if (MyLogger.getInstance().isError()) logger.print(MyLogger.ERROR, "WekaModelBuilder::evaluate::"+MyUtils.getStackTrace(e));
			out = false;
		}
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "WekaModelBuilder::evaluate:: Evaluating:["+out+"]");
		return out;
	}
	
	public static boolean buildModel()
	{
		String config = "\\conf\\training.conf";
		return WekaModelBuilder.buildModel(LoadConfigurations.trainingConfigType, config);
	}
	
	public static boolean buildModel(String configType, String config)
	{
		boolean out = false;
		MyLogger logger = MyLogger.getInstance();
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::buildModel");
		
		WekaModelBuilder wmb = new WekaModelBuilder(configType, config);
		out = wmb.trainModel();
		if (out)
		{
			out = wmb.storeModel();
		}
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "WekaModelBuilder:: Build Model ["+out+"]");
		return out;
	}
	
	public static boolean evaluateModel()
	{
		String config = "\\conf\\training.conf";
		return WekaModelBuilder.evaluateModel(LoadConfigurations.trainingConfigType,config);
	}
	
	public static boolean evaluateModel(String configType, String config)
	{
		boolean out = false;
		MyLogger logger = MyLogger.getInstance();
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::evaluateModel");
		
		WekaModelBuilder wmb = new WekaModelBuilder(configType, config);
		out = wmb.evaluate();
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "WekaModelBuilder:: Evaluate Model ["+out+"]");
		return out;
	}
	
	public static void main(String[] args) throws Exception 
	{
		MyLogger logger = MyLogger.getInstance();
		logger.setLogFileName(System.getProperty("user.dir") + "\\WekaModelBuilder.log");
		logger.setLogLevel(MyLogger.DEBUG);
		logger.open();
		
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::main");	
		
		String config = "\\conf\\training.conf";
		WekaModelBuilder we = new WekaModelBuilder(LoadConfigurations.trainingConfigType,config);
		
		we.trainModel();
		
		WekaModelPredictor.predict();
		
	//	String dataInputDirectory = "\\\\192.168.1.40\\home\\TFM\\Data\\WEKA\\training\\";
	//	WekaExecutor.Method01(dataInputDirectory);
		
		logger.close();
	}
	
	/*
	public static void Method01(String dataInputFile) throws Exception 
	{
		MyLogger logger = MyLogger.getInstance();
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "WekaModelBuilder::connect");
		String trainingDataFile = dataInputFile;
        // Create trainning data instance
        TextDirectoryLoader loader = new TextDirectoryLoader();
        loader.setDirectory(new File(trainingDataFile));
        Instances dataRaw = loader.getDataSet();

        StringToWordVector filter = new StringToWordVector();
        filter.setInputFormat(dataRaw);
        Instances dataTraining = Filter.useFilter(dataRaw, filter);
        //dataTraining.setClassIndex(dataRaw.numAttributes() - 1);

        // Create test data instances
        loader.setDirectory(new File("\\\\192.168.1.40\\home\\TFM\\Data\\WEKA\\test\\"));
        dataRaw = loader.getDataSet();
        Instances dataTest = Filter.useFilter(dataRaw, filter);
        //dataTest.setClassIndex(dataTest.numAttributes() - 1);


        // Classify
        FilteredClassifier model = new FilteredClassifier();
        model.setFilter(new StringToWordVector());
        model.setClassifier(new M5Rules());
        model.buildClassifier(dataTraining);

        for (int i = 0; i < dataTest.numInstances(); i++) 
        {
             dataTest.instance(i).setClassMissing();
             double cls = model.classifyInstance(dataTest.instance(i));
             dataTest.instance(i).setClassValue(cls);
             System.out.println(dataTest.instance(i).toString() + " | " + cls);
             System.out.println(cls + " -> " + dataTest.instance(i).classAttribute().value((int) cls));

            // evaluate classifier and print some statistics
             Evaluation eval = new Evaluation(dataTraining);
             eval.evaluateModelOnce(cls, dataTest.instance(i));
             System.out.println(eval.toSummaryString("\nResults\n======\n", false));
        }
	}
	*/
	
}
