package com.osbs.usermodel.startup;

import java.util.ArrayList;

import com.osbs.usermodel.tools.HiveConnector;
import com.osbs.usermodel.tools.LoadConfigurations;
import com.osbs.utils.MyLogger;

public class HiveModelBuilder 
{

	HiveConnector hc = null;
	MyLogger logger = MyLogger.getInstance();
	String serdesLib = null;
	String hiveStorage = null;
	String database = null;
	
	private HiveModelBuilder(String usermodel)
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveModelBuilder");
		
		LoadConfigurations.getInstance().loadConfig(LoadConfigurations.usermodelConfigType, usermodel);
		serdesLib = LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "serdesLib");
		hiveStorage =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "hive.storage");
		database =  LoadConfigurations.getInstance().getProperty(LoadConfigurations.usermodelConfigType, "database.database");

		hc = new HiveConnector(usermodel);
		
		if (logger.isDebug())
		{
			logger.print(MyLogger.DEBUG, "HiveModelBuilder:: serdesLib::["+serdesLib+"]");
			logger.print(MyLogger.DEBUG, "HiveModelBuilder:: hiveStorage::["+hiveStorage+"]");
			logger.print(MyLogger.DEBUG, "HiveModelBuilder:: database::["+database+"]");
		}
	}
	
	private boolean connect() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveModelBuilder::connect");
		boolean out = hc.connect();
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveModelBuilder::connect:: Connect["+out+"]");
		return out;
	}
	
	private boolean checkConnection() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveModelBuilder::checkConnection");
		boolean out =  hc.checkConnection();
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveModelBuilder::checkConnection:: Check connection["+out+"]");
		return out;
	}
	
	private boolean executeInitialCommands() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveModelBuilder::executeInitialCommands");
		ArrayList<String> cl = new ArrayList<String>();
		cl.add("add jar "+serdesLib+"csv-serde-1.1.2-0.11.0-all.jar");
		cl.add("add jar "+serdesLib+"hive-contrib-0.12.0.2.0.10.0-1.jar");
		cl.add("SET hive.auto.convert.join = false");
		cl.add("CREATE DATABASE IF NOT EXISTS "+ database);
		cl.add("use "+ database);
		
		boolean out = hc.executeCommands(cl);
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveModelBuilder::executeInitialCommands:: Initial Commands Executed["+out+"]");
		return out;
	}
	
	private boolean modelIsAlreadyCreated()throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveModelBuilder::modelIsAlreadyCreated");
		String t1 = "raw_log";
		String t2 = "raw_log_view";
		String t3 = "raw_log_view_hourly";
		String t4 = "SERVERS";
		String t5 = "CALENDAR";
		String t6 = "calendar_view";
		String t7 = "WORKINGHOURS";
		String t8 = "workinghours_view";
		String t9 = "access_info";
		String t10 = "previous_festive_view";
		String t11 = "aggregated_info";
		
		ArrayList<String> tables = new ArrayList<String>();
		tables.add(t1);
		tables.add(t2);
		tables.add(t3);
		tables.add(t4);
		tables.add(t5);
		tables.add(t6);
		tables.add(t7);
		tables.add(t8);
		tables.add(t9);
		tables.add(t10);
		tables.add(t11);
		
		return hc.existsThisTables(tables);
	}
	private boolean createModel() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveModelBuilder::createModel");

		String t1 =  "CREATE EXTERNAL TABLE IF NOT EXISTS raw_log ( syslogdate STRING, sysloghost STRING, syslogprogram STRING, nd1 STRING, logonid STRING, systemid STRING, eventid STRING, eventtype STRING, controller STRING, host STRING, address STRING, record STRING, user STRING, domain STRING, source STRING, category STRING, type STRING, id STRING, generated STRING, written STRING, message STRING, username STRING, domainname STRING, sessionid STRING, sessiontype STRING, process STRING, authenticationtype STRING, workstation STRING, guid STRING, calleruser STRING, callerdomain STRING, callersessionid STRING, callerprocess STRING, services STRING, ip STRING, originport STRING ) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' WITH SERDEPROPERTIES ( \"input.regex\" = \"(.{15})\\\\s(.*)\\\\s(.*):(\\\\s*)(\\\\d*)\\\\|(\\\\d*)\\\\|(\\\\d*)\\\\|(.*)\\\\|(.*)\\\\|Host:(.*?)\\\\s*?Address:(.*?)\\\\s.*Record:(.*?)\\\\s*?User:(.*?)\\\\s*?Domain:(.*)\\\\sSource:(.*)\\\\sCategory:(.*)\\\\sType:(.*)\\\\sId:(.*)\\\\sGenerated:(.*)\\\\sWritten:(.*?)\\\\sMessage:([^:]*):[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:\\\\s*(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:\\\\s*(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:\\\\s*(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:\\\\s*(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:\\\\s*(?:\\\\[09)*([^\\\\[]*)\\\\[0D[^:]*:(?:\\\\[09)*([^\\\\[]*)\\\\[0D.*\", \"output.format.string\" = \"%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s %13$s %14$s %15$s %16$s %17$s %18$s %19$s %20$s %21$s %22$s %23$s %24$s %25$s %26$s %27$s %28$s %29$s %30$s %31$s %32$s %33$s %34$s %35$s %36$s\" ) LOCATION '"+hiveStorage+"raw_logs'";
		String t2 =  "CREATE VIEW raw_log_view AS SELECT CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(generated, 'EEE MMM dd HH:mm:ss yyyy')) AS TIMESTAMP) AS generated, user, ip FROM raw_log";
		String t3 =  "CREATE VIEW raw_log_view_hourly AS SELECT CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(YEAR(generated),\"-\",MONTH(generated),\"-\",DAY(generated),\" \",HOUR(generated),\":00:00\"), 'yyyy-MM-dd HH:mm:ss')) AS TIMESTAMP) AS generated, user, ip FROM raw_log_view";
		String t4 =  "CREATE EXTERNAL TABLE SERVERS ( ip STRING,server STRING ) ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde' STORED AS TEXTFILE LOCATION '"+hiveStorage+"machines'";
		String t5 =  "CREATE EXTERNAL TABLE CALENDAR ( fecha STRING,tipo STRING ) ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde' STORED AS TEXTFILE LOCATION '"+hiveStorage+"calendar'";
		String t6 =  "CREATE VIEW calendar_view AS SELECT CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(fecha, 'yyyy-MM-dd')) AS TIMESTAMP) AS fecha, tipo FROM calendar";
		String t7 =  "CREATE EXTERNAL TABLE WORKINGHOURS ( tipo STRING, inicio STRING,fin STRING ) ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde' STORED AS TEXTFILE LOCATION '"+hiveStorage+"workinghours'";
		String t8 =  "CREATE VIEW workinghours_view AS SELECT tipo, CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(inicio, 'yyyy-MM-dd HH:mm:ss')) AS TIMESTAMP) AS inicio, CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(fin, 'yyyy-MM-dd HH:mm:ss')) AS TIMESTAMP) AS fin FROM workinghours";
		String t9 =  "CREATE VIEW access_info AS SELECT generated, fecha, IF(tipo = 'FESTIVO', 'FESTIVO',IF(tipo = 'REDUCIDA', 'REDUCIDA', IF(from_unixtime(unix_timestamp(generated), 'EEEE') = 'Friday', 'VIERNES', IF((from_unixtime(unix_timestamp(generated), 'EEEE') = 'Saturday') OR (from_unixtime(unix_timestamp(generated), 'EEEE') = 'Sunday'), 'FINDESEMANA', 'NORMAL')))) as eltipo, IF(tipo = 'FESTIVO', 'true', 'false') as isFestive, IF(tipo = 'REDUCIDA', 'true', 'false') as isReduced, IF(from_unixtime(unix_timestamp(generated), 'EEEE') = 'Friday', 'true', 'false') as isFriday, IF((from_unixtime(unix_timestamp(generated), 'EEEE') = 'Saturday') OR (from_unixtime(unix_timestamp(generated), 'EEEE') = 'Sunday'), 'true', 'false') as isWeekend, COALESCE(server,'NO-SERVER') as elserver, count(*) as conexiones FROM raw_log_view_hourly LEFT JOIN servers ON (raw_log_view_hourly.ip = servers.ip) LEFT JOIN calendar_view ON (to_date(calendar_view.fecha) = to_date(raw_log_view_hourly.generated)) WHERE (generated IS NOT NULL) GROUP BY generated, fecha, tipo, server";
		String t10 = "CREATE VIEW previous_festive_view AS SELECT DATE_SUB(fecha,1) previous FROM calendar_view WHERE tipo = 'FESTIVO'";
		String t11 = "CREATE VIEW aggregated_info AS SELECT YEAR(access_info.generated) as year, MONTH(access_info.generated) as month, DAY(access_info.generated) as day, from_unixtime(unix_timestamp(generated), 'EEEE') as weekday, HOUR(access_info.generated) as hour, isFestive, IF(previous_festive_view.previous IS NULL, 'false', 'true') as isPreviousFestive, isWeekend, isFriday, isReduced, IF((eltipo = 'FESTIVO') OR (eltipo = 'FINDESEMANA') ,'false', IF((((eltipo = 'REDUCIDA') OR (eltipo = 'VIERNES') OR (eltipo = 'NORMAL') ) AND (HOUR(access_info.generated) < HOUR(inicio) OR HOUR(access_info.generated) > HOUR(fin))) ,'false' ,'true' ) ) as isWorkingHour, elserver as server, conexiones as connections FROM access_info LEFT JOIN workinghours ON (access_info.eltipo = workinghours.tipo) LEFT JOIN previous_festive_view ON (YEAR(previous_festive_view.previous)=YEAR(access_info.generated) AND MONTH(previous_festive_view.previous) = MONTH(access_info.generated) AND DAY(previous_festive_view.previous) = DAY(access_info.generated)) WHERE (access_info.generated IS NOT NULL)";
		
		ArrayList<String> lModel = new ArrayList<String>();
		lModel.add(t1);
		lModel.add(t2);
		lModel.add(t3);
		lModel.add(t4);
		lModel.add(t5);
		lModel.add(t6);
		lModel.add(t7);
		lModel.add(t8);
		lModel.add(t9);
		lModel.add(t10);
		lModel.add(t11);
		
		boolean out = hc.executeCommands(lModel);
		
		if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveModelBuilder::createModel:: Model Created["+out+"]");
		return out;
	
	}
	
	
	private boolean close() throws Exception
	{
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveModelBuilder::close");
		return hc.close();
	}
	
	public static boolean buildModel() throws Exception
	{
		boolean out = false;
		MyLogger logger = MyLogger.getInstance();
		if (MyLogger.getInstance().isDebug()) logger.print(MyLogger.DEBUG, "HiveModelBuilder::buildModel");
		
		String usermodel = "\\conf\\usermodel.conf";
		
		HiveModelBuilder hmb = new HiveModelBuilder(usermodel);
		hmb.connect();
		hmb.checkConnection();
		hmb.executeInitialCommands();
		if (!hmb.modelIsAlreadyCreated())
		{
			out = hmb.createModel();
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveModelBuilder::buildModel::Created::["+out+"]");
		}
		else
		{
			if (MyLogger.getInstance().isInfo()) logger.print(MyLogger.INFO, "HiveModelBuilder::buildModel::[Already created!]");
		}
		hmb.close();
		return out;
	}

}
