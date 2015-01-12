/**
 * 
 */
package tools;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.TimeZone;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import compare.CompareData;

import persistence.AppParameters;
import persistence.DelayObject;
import persistence.DifferenceObject;
import persistence.SumObject;
import service.CoreTAService;
import service.GPService;

/**
 * Utility class to have common static variables and methods
 * 
 * @author vivek.subedi
 *
 */
public class CommonCompareUtil {
	
	private static Logger logger = Logger.getLogger(CommonCompareUtil.class);
	
	//Drivers
	public static final String DB2JDBCDRIVER = "com.ibm.db2.jcc.DB2Driver";
	public static final String GPCONNECTIONURL = "jdbc:pivotal:greenplum://";
	public static final String GPJDBCDRIVER= "com.pivotal.jdbc.GreenplumDriver";
	
	//CONSTANTS
	public static final String CORETA = "CORETA";
	public static final String TAGP = "TAGP";
	public static final String COREGP = "COREGP";
	public static final String ARCHIVE = "ARCHIVE";
	public static final String LONGHISTORY = "COUNT_LEGACY_COD_WEB_LOGIN_HISTORY";
	
	
	//Variables
	private static LinkedHashSet<String> errorTables = new LinkedHashSet<String>(); 

	/**
	 * Gets the stack trace of a throwable exception.
	 *
	 * @param throwable the exception
	 * @return String of stack trace
	 */
	public static String getStackTrace(final Throwable throwable) {
		final StringWriter sw = new StringWriter();
	    final PrintWriter pw = new PrintWriter(sw, true);
	    throwable.printStackTrace(pw);
	     
	    return sw.getBuffer().toString();
	}
	
	//checks for the daylight saving for the date of its run.
	public static Integer checkDayLightSaving() {
		Integer hourNeedsTobeAdded;
		Boolean dayLightSaving = TimeZone.getDefault().inDaylightTime(new Date());
		if (dayLightSaving) {
			hourNeedsTobeAdded = 4;
		} else {
			hourNeedsTobeAdded = 5;
		}
		
		return hourNeedsTobeAdded;
	}
	
	public static Map<String, LinkedList<String>> getSumColumnMap(LinkedList<String> selectedTableList, GPService gpService, CoreTAService coreTAService, 
			AppParameters parameters){
		Map<String, LinkedList<String>> sumColumnsMap = null;
		try { 
			if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.TAGP)) {
				sumColumnsMap = gpService.getTASumColumns(parameters.getRegion(), selectedTableList);
			} else if(parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.COREGP)){
				sumColumnsMap = coreTAService.getSumColumns(parameters.getRegion(), selectedTableList);
			} else {
				sumColumnsMap = coreTAService.getSumColumns(parameters.getRegion(), selectedTableList);
			}
		} catch (NullPointerException e) {
			logger.debug("The table you are trying to compare doesn't have any columns that can be summed. We will just count table "+e.getMessage());
			logger.debug(CommonCompareUtil.getStackTrace(e));
		} catch ( SQLException  e2) {
			logger.debug("Something went wrong while quering the sum columns. Please check your query."+e2.getMessage());
			logger.debug(CommonCompareUtil.getStackTrace(e2));
		}
		return sumColumnsMap;
	}
	
	public static Map<String, LinkedList<String>> getArchiveSumColumnMap(LinkedList<String> selectedTableList, CoreTAService coreTAService, 
			AppParameters parameters){
		Map<String, LinkedList<String>> sumColumnsMap = null;
		try { 
			sumColumnsMap = coreTAService.getArchiveSumColumns(parameters.getRegion(), selectedTableList);
		} catch (NullPointerException e) {
			logger.debug("The table you are trying to compare doesn't have any columns that can be summed. We will just count table "+e.getMessage());
			logger.debug(CommonCompareUtil.getStackTrace(e));
		} catch ( SQLException  e2) {
			logger.debug("Something went wrong while quering the sum columns. Please check your query."+e2.getMessage());
			logger.debug(CommonCompareUtil.getStackTrace(e2));
		}
		return sumColumnsMap;
	}
	
	public static LinkedHashSet<String> getAllTablesWithNoSumColumns(LinkedList<String> selectedTableList, GPService gpService, CoreTAService coreTAService, 
			AppParameters parameters, LinkedHashSet<String> coreTables) {
		
		LinkedList<String> noSumColumnsTable = new LinkedList<String>();
		LinkedList<String> updatedCoreTables = new LinkedList<String>();
		
		logger.info("The number of tables that are being processed using sum columns: ["+coreTables.size()+"]");
		for (String string : coreTables) {
			updatedCoreTables.add(CheckSumSQLGenerator.trimTableName(string));
			logger.info(CheckSumSQLGenerator.trimTableName(string));
		}
		
		if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA) || parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.COREGP)) {
			for (String string : selectedTableList) {
				if (updatedCoreTables.contains(string)) {
					continue;
				}else {
					if (string.startsWith("ED_")) {
						noSumColumnsTable.add("T"+string);
					} else {
						noSumColumnsTable.add(string);
					}
				}
			}
		} else  {
			for (String string : selectedTableList) {
				if (updatedCoreTables.contains(string)) {
					continue;
				}else {
					noSumColumnsTable.add(string);
				}
			}
		}
		
		/**
		 * Getting table names with schema for those tables which don't have sum columns
		 */
		LinkedList<String> noSumColumnsTablesList = null;
		if (!noSumColumnsTable.isEmpty()) {
			try {
				if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.COREGP)) {
					noSumColumnsTablesList = coreTAService.getCountTablesList(parameters.getRegion(), noSumColumnsTable);
				}
				else if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA)) {
					 noSumColumnsTablesList = coreTAService.getCountTablesList(parameters.getRegion(), noSumColumnsTable);
				} else {
					noSumColumnsTablesList = gpService.getCountTablesList(parameters.getRegion(), noSumColumnsTable);
				}
			} catch (SQLException e) {
				logger.info(CommonCompareUtil.getStackTrace(e));
			}
		}
		
		//Adding back to the coreTables list so that we can porcess those tables which don't have any sum columns as well
		logger.info("The number of tables those don't have sum columns. And doing only count on those tables are as follows:");
		if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.TAGP)) {
			if (!noSumColumnsTable.isEmpty()) {
				for (String string : noSumColumnsTable) {
					coreTables.add(string);
					logger.info(string);
				}
			}
		} else {
			if (noSumColumnsTablesList != null) {
				for (String string : noSumColumnsTablesList) {
					coreTables.add(string);
					logger.info(string);
				}
			}
		}
		return coreTables;
	}
	
	public static LinkedHashSet<String> getAllArchiveTablesWithNoSumColumns(LinkedList<String> selectedTableList, CoreTAService coreTAService, 
			AppParameters parameters, LinkedHashSet<String> coreTables) {
		
		LinkedList<String> noSumColumnsTable = new LinkedList<String>();
		
		logger.info("The number of tables that are being processed using sum columns: ["+coreTables.size()+"]");
		for (String string : coreTables) {
			logger.info(string);
		}
		for (String string : selectedTableList) {
			if (coreTables.contains(string)) {
				continue;
			}else {
				noSumColumnsTable.add(string);
			}
		}
		//setting the table list that that doesn't have sum columns
		CheckSumSQLGenerator.setCountTablesList(noSumColumnsTable);
		
		//Adding back to the coreTables list so that we can porcess those tables which don't have any sum columns as well
		logger.info("The number of tables those don't have sum columns. And doing only count on those tables are as follows:");
		if (noSumColumnsTable != null) {
			for (String string : noSumColumnsTable) {
				coreTables.add(string);
				logger.info(string);
			}
		}
		return coreTables;
	} 
	
	public static Map<String, LinkedList<String>> getPrimaryKeyMap(LinkedHashSet<String> coreTables, GPService gpService, CoreTAService coreTAService, 
			AppParameters parameters){
		Map<String, LinkedList<String>> primayKeyColumnMap = null;
		if (!coreTables.isEmpty()) {
			try {
				if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.COREGP) || parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.TAGP)) {
					primayKeyColumnMap = gpService.getPrimaryKeyTableMap(parameters.getRegion(), coreTables);
				} 
				if(parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA)){
					primayKeyColumnMap = coreTAService.getPrimaryKeyTableMap(parameters.getRegion(), coreTables);
				} 
			} catch (SQLException e) {
				logger.info(CommonCompareUtil.getStackTrace(e));
			}
		} else {
			logger.debug("There are not any tables to process. Table might not have any columns to sum !");
			logger.debug("Program is returning...");
		}
		return primayKeyColumnMap;
	}
	
	
	/**
	 * Find differences. Process the CORE, TA and Greenplum results to find the difference
	 *
	 * @param allQueryResultVector - holds all the resultset from core, ta and greenplum depending on the user choice of comparison
	 * @return the map -  Map of the differences with table name
	 */
	public static Map<String, LinkedList<DifferenceObject>> findDifferences(Vector<DelayObject> allQueryResultVector, AppParameters parameters) {
		Map<String, LinkedList<DifferenceObject>> differenceMap = new HashMap<String, LinkedList<DifferenceObject>>();
		logger.debug("Calculating Differences between two resultsets");
		for (DelayObject delayObject : allQueryResultVector) {
			Map<String, Map<String, SumObject>> coreTaSideSumMap = delayObject.getCoreSideSumObjectMap();
			Map<String, Map<String, SumObject>> taGPSideSumMap = delayObject.getTaSideSumObjectMap();
			if (coreTaSideSumMap != null) {
				Iterator<Entry<String, Map<String, SumObject>>> calculateDifferenceIterator = coreTaSideSumMap.entrySet().iterator();
				
				//loops through the iterator
				String tableNameString = null;
				while (calculateDifferenceIterator.hasNext()) {
					Entry<String, Map<String, SumObject>> entry = calculateDifferenceIterator.next();
					tableNameString = entry.getKey();
					Map<String, SumObject> coreTaSumObjectsMap = entry.getValue();
					Map<String, SumObject> taGPSumObjectsMap = taGPSideSumMap.get(tableNameString);
					
					if (coreTaSumObjectsMap != null) {
						if (taGPSumObjectsMap != null) {
							//finds the difference of the CORE and TA Values of resultsets
							LinkedList<DifferenceObject> difference = CompareData.compareDataMap(coreTaSumObjectsMap, taGPSumObjectsMap, parameters);
							if (!difference.isEmpty()) {
								differenceMap.put(tableNameString, difference);
							}
						} else {
							logger.error("Can't execute Query for ["+tableNameString +"] table. Please check if the table is in the schema and your permission!!");
							continue;
						}
					} else {
						logger.error("Something went wrong while exectuing query in DB2. Result set is null. Please check the table!!");
					}
				}
				logger.debug("Done Comparing ["+tableNameString+"] table");
			} else {
				logger.error("["+delayObject.getTablename()+"] can't be read. Please check the query.");
				continue;
			}
			
		}
		return differenceMap;
	}
	
	public synchronized static void setErrorTables(String tableName) {
		errorTables.add(tableName);
	}
	
	public static LinkedHashSet<String> getErrorTables(){
		return errorTables;
	}
}
