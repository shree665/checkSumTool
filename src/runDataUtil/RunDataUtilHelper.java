/**
 * 
 */
package runDataUtil;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.swing.DefaultListModel;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.log4j.Logger;

import concurrency.TAGPWorkerThread;
import concurrency.WorkerThread;
import email.EmailAttachmentSender;
import excelfactory.ExcelReportFactory;
import persistence.AppParameters;
import persistence.DelayObject;
import persistence.DifferenceObject;
import persistence.SumObject;
import service.CoreTAService;
import service.GPService;
import tools.CheckSumSQLGenerator;
import tools.CommonCompareUtil;

/**
 * @author vivek.subedi
 *
 */
public class RunDataUtilHelper {
	
	private static Logger logger = Logger.getLogger(RunDataUtilHelper.class);
	
	private static DefaultListModel<Object> addedRepModel;
	private static DefaultListModel<Object> doneModel;
	private static Map<String, LinkedList<String>> sumColumnsMap;
	private static Map<String, LinkedList<String>> primayKeyColumnMap;
	private static LinkedHashSet<String> coreTables;
	protected Map<String, Map<String, SumObject>> coreSideSumObjectMap;
	protected Map<String, Map<String, SumObject>> taSideSumObjectMap;
	private static Integer hourNeedsTobeAdded;
	public static LinkedList<String> noSumColumnsTable;
	private static Boolean commandline = true;
	
	public static void runComparison(LinkedList<String> tableList, AppParameters parameters, CoreTAService coreTAService, GPService gpService, File saveFile) {
		//We process tables if there are any available in the region
		coreTables = new LinkedHashSet<String>();
		String compareType = parameters.getComparisonType();
		if (tableList != null) {
			Long startTime = System.currentTimeMillis();
			noSumColumnsTable = new LinkedList<String>();
			
			//checks for the daylight saving for the date of its run.
			hourNeedsTobeAdded = CommonCompareUtil.checkDayLightSaving();
			
			/**
			 * getting all the columns of tables that can be summed and catching nullpointer exception if the table don't have any columns
			 * that needs to be sum up
			 */
			if (compareType.equalsIgnoreCase(CommonCompareUtil.ARCHIVE)) {
				sumColumnsMap = CommonCompareUtil.getArchiveSumColumnMap(tableList, coreTAService, parameters);
			} else {
				sumColumnsMap = CommonCompareUtil.getSumColumnMap(tableList, gpService, coreTAService, parameters);
			}
			
			if (sumColumnsMap == null) {
				logger.debug("The table you are trying to compare doesn't have any columns that can be summed. We will just do count table");
			}
			
			/**
			 * Adding those tables to the coreTables list. We will use this list to find out the tables which don't have any sum columns in table.
			 * We do only count for those tables
			 */
			if (sumColumnsMap != null) {
				Iterator<Entry<String, LinkedList<String>>> iterator = sumColumnsMap.entrySet().iterator();
				while (iterator.hasNext()) {
					Map.Entry<String, LinkedList<String>> entry = iterator.next();
					String tableName = entry.getKey();
					coreTables.add(tableName);
				}
			}
			
			//Retrieving all tables which don't have sum columns and adding back to the table list to process
			if (compareType.equalsIgnoreCase(CommonCompareUtil.ARCHIVE)) {
				coreTables = CommonCompareUtil.getAllArchiveTablesWithNoSumColumns(tableList, coreTAService, parameters, coreTables);
			} else {
				coreTables = CommonCompareUtil.getAllTablesWithNoSumColumns(tableList, gpService, coreTAService, parameters, coreTables);
			}
			
			//getting primary keys of all those tables which we will be ultimately processing
			if (!compareType.equalsIgnoreCase(CommonCompareUtil.TAGP) || !compareType.equalsIgnoreCase(CommonCompareUtil.ARCHIVE)) {
				primayKeyColumnMap = CommonCompareUtil.getPrimaryKeyMap(coreTables, gpService, coreTAService, parameters);
				if (primayKeyColumnMap != null) {
					logger.info("The number of tables that have primary keys: ["+primayKeyColumnMap.size()+"]");
				} else {
					logger.debug("The table you are trying to compare doesn't have any primary key. No comparison !!");
					throw new IllegalArgumentException("Tables that you are trying to compare don't have any primary key. "
							+ "Since there are not any primary key, no comparison can happen");
				}
			}
			
			//adding archive comparison
		
			/**
			 * creating Queue for Threads. 
			 * MasterQueue - holds all the tables that need to be compare. This queue is linked blocking queue.
			 * allQueryResultVector - allQueryResultVector is a @Vector which holds the final object of core and TA resultsets as a objects
			 */
			
			LinkedBlockingQueue<String> masterQueue = new LinkedBlockingQueue<String>();
			Vector<DelayObject> allQueryResultVector = new Vector<DelayObject>();
			
			//putting all tables that are going to be processed into the masterQueue
			for (String string : coreTables) {
				masterQueue.add(string);
			}
			logger.info("The size of master queue: ["+masterQueue.size()+"]");
			
			if (compareType.equalsIgnoreCase(CommonCompareUtil.ARCHIVE)) {
				Integer numberThreads = 4;
				if (parameters.getMaxNumberofThread() > 0) {
					numberThreads = parameters.getMaxNumberofThread();
				} 
				
				ExecutorService executors = Executors.newFixedThreadPool(numberThreads);
				logger.info("# of active threads: ["+numberThreads+"]");
				CompletionService<String> pool = new ExecutorCompletionService<String>(executors);
				for (int i = 0; i < coreTables.size(); i++) {
					String tablename = null;
					try {
						tablename = masterQueue.take();
						Runnable worker = new WorkerThread(tablename, parameters, sumColumnsMap.get(tablename), allQueryResultVector, addedRepModel, commandline);
						pool.submit(worker, tablename);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				logger.info("Finished initializing thread pool !");
				executors.shutdown();
				
				//waiting all threads to be done before the comparison
				try {
					executors.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				} catch (InterruptedException  e) {
					logger.info("Thread gets interepted !");
				}
				logger.info("All Threads are done !");
				logger.info("Excution of Query in TA and Greenplm is completed");
			} else {
				logger.info("The size of master queue: ["+masterQueue.size()+"]");
				ExecutorService executors = Executors.newFixedThreadPool(Integer.valueOf(parameters.getMaxNumberofThread()));
				logger.info("# of active threads: ["+Integer.valueOf(parameters.getMaxNumberofThread())+"]");
				CompletionService<String> pool = new ExecutorCompletionService<String>(executors);
				
				for (int j = 0; j < coreTables.size(); j++) {
					String tablename = null;
					try {
						tablename = masterQueue.take();
						Runnable worker = new TAGPWorkerThread(parameters, sumColumnsMap, primayKeyColumnMap, tablename, Long.valueOf(parameters.getDelayTime()), hourNeedsTobeAdded, 
								addedRepModel, commandline, allQueryResultVector, doneModel);
						pool.submit(worker, tablename);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				logger.info("Finished initializing thread pool");
				executors.shutdown();
				
				//waiting all threads to be done before the comparison
				try {
					executors.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				} catch (InterruptedException  e) {
					logger.info("Thread gets interepted !");
				}
				logger.info("Finished all threads");
			}
			
			logger.info("Parent Thread Resume work to compare those resultsets");
			logger.info("Processing Differences...");
			
			//Compares the core, TA and Greenplum resultsets depending upon the comparison type
			Map<String, LinkedList<DifferenceObject>> differencesMap = CommonCompareUtil.findDifferences(allQueryResultVector, parameters);
			logger.info("Comparison has been completed");
			
			logger.debug("Writing into ["+saveFile.getName()+"] file which is located in ["+saveFile.getAbsolutePath()+"]");
			
			//Writes all the value of resultset of CORE and TA to the Excel file
			if (!differencesMap.isEmpty()) {
				ExcelReportFactory.generateReport(differencesMap, saveFile, parameters.getComparisonType());
				logger.info(saveFile.getName()+" is successfully created in ["+saveFile.getPath()+"] directory");
			} else {
				logger.info("There is not anything to write....");
				return;
			}
			
			//Sending Email
			EmailAttachmentSender.sendEmail(saveFile, parameters.getRegion());
			
			//Creating list to put the tables that are done comparing
			LinkedList<String> doneTableList = getDoneList(differencesMap);
			
			logger.info("The total number of tables that were processed and compared: ["+doneTableList.size()+"]");
			logger.info("Following tables are processed and compared:\n");
			for (int i=0; i<doneTableList.size(); i++) {
				logger.info("["+(i+1) + "]  - " +CheckSumSQLGenerator.trimTableName(doneTableList.get(i)));
			}
			
			if (!CommonCompareUtil.getErrorTables().isEmpty()) {
				logger.info("["+CommonCompareUtil.getErrorTables().size()+ "] Following Tables done with errors. Please check log for the error:\n");
				for (String tableName : CommonCompareUtil.getErrorTables()) {
					logger.info("["+tableName+"]");
				}
			}
			logger.info("COMPLETED");
			Long endTime = System.currentTimeMillis();
			logger.info("The total time to process ["+doneTableList.size()+"] tables is: ["+DurationFormatUtils.formatDuration((endTime - startTime), "HH:mm:ss")+"] seconds");
		} 
	}
	
	/**
	 * Gets the List of tables that are compared.
	 *
	 * @param differencesMap - @Map of tables and its differences
	 * @return @LinkedList of table name that are compared
	 */
	private static LinkedList<String> getDoneList(Map<String, LinkedList<DifferenceObject>> differencesMap) {
		LinkedList<String> doneTableList = new LinkedList<String>();
		Iterator<Entry<String, LinkedList<DifferenceObject>>> doneList = differencesMap.entrySet().iterator();
		while (doneList.hasNext()) {
			Map.Entry<String, LinkedList<DifferenceObject>> entry = doneList.next();
			doneTableList.add(entry.getKey());
			
		}
		return doneTableList;
	}

}
