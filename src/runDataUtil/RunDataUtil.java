/**
 * 
 */
package runDataUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import gui.DataUtilFrame;

import com.beust.jcommander.JCommander;

import persistence.AppParameters;
import persistence.DatabaseRegion;
import persistence.SumObject;
import security.DecryptPassword;
import service.CoreTAService;
import service.GPService;
import tools.CommonCompareUtil;
import tools.DB2Connection;
import tools.GPConnection;

/**
 * @author vivek.subedi
 *
 */
public class RunDataUtil {
	
	private static Logger logger = Logger.getLogger(RunDataUtil.class);
	
	private static CoreTAService coreTAService;
	private static GPService gpService;
	private static String compareType;
	protected Map<String, Map<String, SumObject>> coreSideSumObjectMap;
	protected Map<String, Map<String, SumObject>> taSideSumObjectMap;
	public static LinkedList<String> noSumColumnsTable;
	private static String filedb2Username;
	private static String filedb2Password;
	private static String filegpPreprodUsername;
	private static String filegpPreprodPassword;
	private static String filegpProdUsername;
	private static String filegpProdPassword;
	private static DatabaseRegion databaseRegion;
	
	public static void main(String[] args) throws Exception {
		//check for GUI. if there is not any arguments, it launchs the GUI
		if (args.length < 2) {
			DataUtilFrame.main(args);
			return;
		}
		
		//else it uses commandline
		AppParameters parameters = new AppParameters();
		try {
			JCommander jCommander = new JCommander(parameters, args);
			if (parameters.isDisplayHelp()) {
				jCommander.usage();
				return;
			}
			try {
				Properties config = new Properties();
				InputStream in = new FileInputStream("checksumdb.properties");
				//InputStream in = RunDataUtil.class.getClassLoader().getResourceAsStream("credential.properties");
				config.load(in);
				DecryptPassword decryptPassword = new DecryptPassword("checksumdb.properties");
				filedb2Username = config.getProperty("db2.username");
				filedb2Password = decryptPassword.decryptPropertyValue("db2.password");
				filegpPreprodUsername = config.getProperty("gp.preprod.username");
				filegpPreprodPassword = decryptPassword.decryptPropertyValue("gp.preprod.password");
				filegpProdUsername = config.getProperty("gp.prod.username");
				filegpProdPassword = decryptPassword.decryptPropertyValue("gp.prod.password");
				
				//loading database connection string and other config values
				InputStream configInput = new FileInputStream("checksumconfig.properties");
				//InputStream configInput = RunDataUtil.class.getClassLoader().getResourceAsStream("config.properties");
				config.load(configInput);
				parameters.setDelayTime(Integer.parseInt(config.getProperty("delay.time")));
				parameters.setMaxNumberofThread(Integer.parseInt(config.getProperty("max.number.thread")));
				parameters.setOutputDirectoryPath(config.getProperty("output.directory.path"));
				String connectionString = config.getProperty(parameters.getDisplay().toString());
				String[] parts = connectionString.split(",");
				if (parts.length != 8) {
					logger.info("All the property are not set in the file. Please check it again !");
					return;
				} else {
					databaseRegion = new DatabaseRegion();
					databaseRegion.setCoreConnectionString(parts[0].trim());
					databaseRegion.setTaConnectionString(parts[1].trim());
					databaseRegion.setGreenplumConnectionString(parts[2].trim());
					databaseRegion.setNonProdOrProdString(parts[3].trim());
					databaseRegion.setDisplay(parts[4].trim());
					databaseRegion.setCore(parts[5].trim());
					databaseRegion.setTa(parts[6].trim());
					databaseRegion.setRegion(parts[7].trim());
				}
				parameters.setDb2Username(filedb2Username.trim());
				parameters.setDb2Password(filedb2Password.trim());
				if (parameters.getDisplay().toString().equalsIgnoreCase("P2396")) {
					 parameters.setGpUsername(filegpProdUsername.trim());
					 parameters.setGpPassword(filegpProdPassword.trim());
				} else {
					parameters.setGpUsername(filegpPreprodUsername.trim());
					parameters.setGpPassword(filegpPreprodPassword.trim());
				}
				
				parameters.setRegion(databaseRegion);
				
			} catch (IOException e) {
				logger.info("Can not read the properties file. Please make sure the properties file in right place and in right format !");
				new IllegalArgumentException("Can not read the properties file. Please make sure the properties file in right place and in right format !");
				return;
			}
		} catch (Exception e) {
			logger.debug(CommonCompareUtil.getStackTrace(e));
			throw new Exception("Insufficient parameter values specified. Please use --help", e);
		}
		
		File saveFile = new File(parameters.getOutputDirectoryPath());
		logger.debug("The input filename and its path [" + saveFile.getAbsolutePath()+"]");
		
		//Checking for the file extension. If user do not chooses anything, default is .xlsx
		if (!saveFile.getName().endsWith(".xlsx")) {
			saveFile = new File(saveFile.getAbsolutePath()+".xlsx");
			logger.info("The file will be saved as: "+ saveFile.getAbsolutePath());
		}
		
		//checking if the file can be written in desired location. Since JDK 1.6 have bug to check for the canWrite() method. Using different approach to it.
		try {
			FileOutputStream outputStream = new FileOutputStream(saveFile);
			outputStream.close();
		} catch (FileNotFoundException e) {
			logger.info("["+saveFile.getPath()+"] file can't be created. Please check your permission to create it and write it in the directory you specified !");
			throw new IllegalArgumentException("Can't write file in ["+saveFile.getPath()+"]. \nPlease check the path and make sure you have permission to write"
					+ " in that directory");
		} catch (IOException e) {
			logger.debug("Couldn't write into the "+ saveFile.getAbsolutePath()+" file.");
			throw new IllegalArgumentException("Can't write file in ["+saveFile.getPath()+"]. \nPlease check the path and make sure you have permission to write"
					+ " in that directory");
		}
		
		//Initializing the database connection to test the connection
		initializeDatabaseConnections();
		
		//Connection check for the right login credentials. If the connection failed, appplication will exits so that user have to run it again 
		if (!DB2Connection.testConnection(parameters.getRegion(), parameters.getDb2Username(), parameters.getDb2Password())) {
			logger.info("DB2 credentials are invalid. Please check your login information");
			throw new IllegalArgumentException("DB2 credentials are invalid. Please DB2 check your login information");
		}
		
		if (!GPConnection.testConnection(parameters.getRegion(), parameters.getGpUsername(), parameters.getGpPassword())) {
			logger.info("Greenplum credentials are invalid. Please check your login information");
			throw new IllegalArgumentException("Greenplum credentials are invalid. Please Greenplum check your login information");
		}
		
		compareType = parameters.getComparisonType();
		
		//Initializing all services
		initilizeService(parameters);
		
		LinkedList<String> repTablesList = null;
		LinkedList<String> archiveTableList = null;
		if (compareType.equalsIgnoreCase(CommonCompareUtil.ARCHIVE)) {
			//getting archive tables of the region
			archiveTableList = getArchiveTables(parameters.getRegion());
			logger.info("Number of Tables that are in archive: ["+ archiveTableList.size()+"]");
			RunDataUtilHelper.runComparison(archiveTableList, parameters, coreTAService, gpService, saveFile);
		} else {
			//getting rep tables of the region
			repTablesList = getRepTables(parameters.getRegion());
			logger.info("Number of Tables that are being replicate each night: ["+ repTablesList.size()+"]");
			RunDataUtilHelper.runComparison(repTablesList, parameters, coreTAService, gpService, saveFile);
		}
	}
	

	/**
	 * Gets the rep tables based on provided database region.
	 *
	 * @param region the region
	 * @return @LinkedList of reptables
	 */
	private static LinkedList<String> getRepTables(DatabaseRegion region) {
		
		LinkedList<String> tableList = null;
		if (compareType.equalsIgnoreCase(CommonCompareUtil.CORETA)) {
			try {
				coreTAService.repTableNames(region);
			} catch (SQLException e1) {
				logger.info(CommonCompareUtil.getStackTrace(e1));
				throw new IllegalArgumentException("Please check your SQL. SQL to get reptables didn't executed properly");
			}
			tableList = coreTAService.getTableNameList();
		}
		if (compareType.equalsIgnoreCase(CommonCompareUtil.TAGP)) {
			try {
				gpService.taRepTableNames(region);
			} catch (SQLException e) {
				logger.info(CommonCompareUtil.getStackTrace(e));
				throw new IllegalArgumentException("Please check your SQL. SQL to get reptables didn't executed properly");
			}
			tableList = gpService.getTableNameList();
		} 
		if (compareType.equalsIgnoreCase(CommonCompareUtil.COREGP)) {
			try {
				gpService.coreRepTableNames(region);
			} catch (SQLException e) {
				logger.info(CommonCompareUtil.getStackTrace(e));
				throw new IllegalArgumentException("Please check your SQL. SQL to get reptables didn't executed properly");
			}
			tableList = gpService.getTableNameList();
		} 
		
		return tableList;
	}
	
	/**
	 * Gets the arcive tables based on provided database region.
	 *
	 * @param region the region
	 * @return @LinkedList of archive tables
	 */
	private static LinkedList<String> getArchiveTables(DatabaseRegion region) {
		
		LinkedList<String> tableList = null;
		try {
			coreTAService.archiveTableNames(region);
		} catch (SQLException e1) {
			logger.info(CommonCompareUtil.getStackTrace(e1));
			throw new IllegalArgumentException("Please check your SQL. SQL to get reptables didn't executed properly");
		}
		
		tableList = coreTAService.getTableNameList();
		
		return tableList;
	}


	//Initializing CoreTAService, GPService
	private static void initilizeService(AppParameters parameters) {
		if (compareType.equalsIgnoreCase(CommonCompareUtil.CORETA)) {
			coreTAService = new CoreTAService(parameters.getDb2Username(), parameters.getDb2Password(), parameters.getRegion());
		} 
		else if (compareType.equalsIgnoreCase(CommonCompareUtil.TAGP)) {
			gpService = new GPService(parameters.getDb2Username(), parameters.getDb2Password(), parameters.getGpUsername(), parameters.getGpPassword(), parameters.getRegion());
		}
		else if (compareType.equalsIgnoreCase(CommonCompareUtil.COREGP)) {
			gpService = new GPService(parameters.getDb2Username(), parameters.getDb2Password(), parameters.getGpUsername(), parameters.getGpPassword(), parameters.getRegion());
			coreTAService = new CoreTAService(parameters.getDb2Username(), parameters.getDb2Password(), parameters.getRegion());
		} else if (compareType.equalsIgnoreCase(CommonCompareUtil.ARCHIVE)) {
			coreTAService = new CoreTAService(parameters.getDb2Username(), parameters.getDb2Password(), parameters.getRegion());
		} else {
			throw new IllegalArgumentException("Please choose right comparison type. Type of comparison are: \nCORETA \n TAGP \n COREGP");
		}
	}
	
	/**
	 * Initialize the Database connections with a JDBC drivers
	 */
	public static void initializeDatabaseConnections() {
		try {
			Class.forName(CommonCompareUtil.DB2JDBCDRIVER);
			Class.forName(CommonCompareUtil.GPJDBCDRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
