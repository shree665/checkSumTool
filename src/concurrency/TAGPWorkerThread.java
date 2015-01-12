/**
 * 
 */
package concurrency;

import excelfactory.DumpDataToTextFile;
import gui.DisplayTableFrame;

import java.io.File;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;

import javax.swing.DefaultListModel;
import javax.swing.SwingUtilities;

import org.apache.log4j.Logger;

import persistence.AppParameters;
import persistence.DelayObject;
import persistence.SumObject;
import service.CoreTAService;
import service.GPService;
import tools.CheckSumSQLGenerator;
import tools.CommonCompareUtil;


/**
 * @author vivek.subedi
 *
 */
public class TAGPWorkerThread implements Runnable {
	
	private static Logger logger = Logger.getLogger(TAGPWorkerThread.class);
	
	private Map<String, LinkedList<String>> sumColumnsMap;
	private Map<String, LinkedList<String>> primaryKeyMap;
	private Vector<DelayObject> allQueryResultVector;
	private Vector<DelayObject> tempResultVector = new Vector<DelayObject>();
	private Long delayTime;
	private Integer hourNeedsTobeAdded;
	private DefaultListModel<Object> addedRepModel;
	private DefaultListModel<Object> doneModel;
	private AppParameters parameters;
	private String tablename;
	private Boolean commandline;
	
	public TAGPWorkerThread(AppParameters parameters, Map<String, LinkedList<String>> sumColumnsMap, Map<String, LinkedList<String>> primaryKeyMap, 
			String tableName, Long delayTime, Integer hourNeedsTobeAdded, DefaultListModel<Object> addedRepModel, Boolean commandline, 
			Vector<DelayObject> allQueryResultVector, DefaultListModel<Object> doneModel) {
		this.sumColumnsMap = sumColumnsMap;
		this.primaryKeyMap = primaryKeyMap;
		this.tablename = tableName;
		this.delayTime = delayTime;
		this.hourNeedsTobeAdded = hourNeedsTobeAdded;
		this.addedRepModel = addedRepModel;
		this.parameters = parameters;
		this.commandline = commandline;
		this.allQueryResultVector = allQueryResultVector;
		this.doneModel = doneModel;
	}
	
	@Override
	public void run() {
		try {
			CoreTAService coreTAService = null;
			
			//checks for the comparison type i.e either for core to ta or others
			coreTAService = new CoreTAService(parameters.getDb2Username(), parameters.getDb2Password().replaceAll("'", ""), parameters.getRegion());
	
		
			LinkedList<String> columns = sumColumnsMap.get(tablename.toUpperCase());
			DelayObject delayObject = new DelayObject(tablename, sumColumnsMap, primaryKeyMap, (long) (0));
			
			//updating the added model with the status of a table
			updateAddedModelStatusOfCORE();

			Map<String, SumObject> coreObject = null;
			try {
				//retrieving sum results in map based on the compare type
				if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA)) {
					coreObject = coreTAService.getCoreSumResultMap(parameters.getRegion(), tablename, columns, hourNeedsTobeAdded);
				} else if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.COREGP)){
					coreObject = coreTAService.getCoreSumResultMap(parameters.getRegion(), tablename, columns, hourNeedsTobeAdded);
				} else  {
					coreObject = coreTAService.getTASumforGP(parameters.getRegion(), tablename, columns, hourNeedsTobeAdded);
				}
			} catch (NullPointerException e) {
				logger.info("["+tablename+"] table query didn't return anything. Please Check your permission on ["+tablename+"] table and its query query.");
				CommonCompareUtil.setErrorTables(tablename);
				logger.debug(CommonCompareUtil.getStackTrace(e));
			} catch (SQLException e) {
				logger.info("["+tablename+"] table query didn't return anything");
				CommonCompareUtil.setErrorTables(tablename);
				logger.debug(CommonCompareUtil.getStackTrace(e));
			} catch (Exception e) {
				logger.info("["+tablename+"] table Query can't run in the database you chose because of ["+e.getMessage()+"] ");
				CommonCompareUtil.setErrorTables(tablename);
				logger.info(CommonCompareUtil.getStackTrace(e));
			}
			
			Map<String, Map<String, SumObject>> coreMap = new HashMap<String, Map<String,SumObject>>();
			coreMap.put(tablename, coreObject);
			delayObject.setCoreSideSumObjectMap(coreMap);
			logger.debug("Inserting Table "+delayObject.getTablename()+" into the TADelayQueue...");
			
			//updating status of tables that are being processed and being processed in core or ta
			updateAddedModelStatusOfTA();
			
			/**
			 * thread sleeps for the user defined time before running the comparison between CORE TO GREENPLUM and  CORE TO TOTAL ACCESS
			 * thread will not sleep when the comparison happened in between TOTAL ACCESS to GREENPLUM
			 */
			if (!parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.TAGP)) {
				Thread.sleep(delayTime * 60 * 1000);
			}
			
			
			LinkedList<String> primaryKeyColumnList = null;
			if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA)) {
				primaryKeyColumnList = primaryKeyMap.get(CheckSumSQLGenerator.trimTableName(tablename));
			} 
			if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.COREGP)) {
				primaryKeyColumnList = primaryKeyMap.get(CheckSumSQLGenerator.trimTableName(tablename));
			}
			
			//checking for the columns that needs to be sum up. If not sending Null so that we can just to count
			LinkedList<String> sumColumnList = null;
			if (sumColumnsMap.get(tablename) != null) {
				sumColumnList = sumColumnsMap.get(tablename);
			}
			
			Map<String, Map<String, SumObject>> coreSumMap = delayObject.getCoreSideSumObjectMap();
			Map<String, SumObject> coreSumResultTable = coreSumMap.get(tablename);
			GPService gpService = null;
			SumObject sumObject = null;
			if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA)) {
				if (coreSumResultTable != null) {
					sumObject = coreSumResultTable.get("COMMIT_DTM");
					coreTAService = new CoreTAService(parameters.getDb2Username(), parameters.getDb2Password().replaceAll("'", ""), parameters.getRegion());
				}
			} else if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.TAGP)) {
				if (coreSumResultTable != null) {
					gpService = new  GPService(parameters.getDb2Username(), parameters.getDb2Password().replaceAll("'", ""), parameters.getGpUsername(),parameters.getGpPassword().replaceAll("'", ""), parameters.getRegion());
				}
			} else {
				if (coreSumResultTable != null) {
					sumObject = coreSumResultTable.get("COMMIT_DTM");
					gpService = new  GPService(parameters.getDb2Username(), parameters.getDb2Password().replaceAll("'", ""), parameters.getGpUsername(),parameters.getGpPassword().replaceAll("'", ""), parameters.getRegion());
				}
			}
			
			Map<String, SumObject> taSumMap = null;
			try {
				if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA)) {
					taSumMap = coreTAService.getTASumResultMap(parameters.getRegion(), tablename, primaryKeyColumnList, sumColumnList, sumObject.getColumnValue());
				} else if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.TAGP)) {
					taSumMap = gpService.getTAGPResultMap(parameters.getRegion(), tablename, sumColumnList);
				} else {
					taSumMap = gpService.getGPofCOREResultMap(parameters.getRegion(), tablename, primaryKeyColumnList, sumColumnList, sumObject.getColumnValue());
				}
				
			} catch (NullPointerException e) {
				logger.info("The query didn't return anything. Please Check your permission on ["+tablename+"] table and its query query.");
				logger.debug(CommonCompareUtil.getStackTrace(e));
			}  catch (Exception e) {
				logger.info("Query can't run in the database you chose because of ["+e.getMessage()+"] ");
				logger.debug(CommonCompareUtil.getStackTrace(e));
			}
			
			Map<String, Map<String, SumObject>> taMap = new HashMap<String, Map<String,SumObject>>();
			taMap.put(tablename, taSumMap);
			delayObject.setTaSideSumObjectMap(taMap);
			allQueryResultVector.add(delayObject);
			tempResultVector.add(delayObject);
			
			//Dumping all the results into the file with every run
			File dumpFile = new File(parameters.getOutputDirectoryPath());
			String newFilePath = dumpFile.getAbsolutePath().replace(dumpFile.getName(), "") + "dumpData.txt";
			DumpDataToTextFile.writeIntoFile(new File(newFilePath), tempResultVector, parameters.getComparisonType());
			logger.info("Dump file has been written..");
			
			//adds to the done model to display in GUI
			updateDoneModel();
			tempResultVector.clear();
	} catch (InterruptedException e) {
		logger.debug("Something went wrong. Here is the exception "+e.getMessage());
		logger.debug(CommonCompareUtil.getStackTrace(e));
	} finally {
		logger.debug("Producer thread is dead.");
	}
		
 }

	
	/**
	 * Updates the done module to display those tables which are done
	 */
	private void updateDoneModel() {
		//new thread to update doneModel in GUI
		if (!commandline) {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					//adding each table to the done model after the query is successfully executed in TOTAL ACCESS
					LinkedList<String> doneList = new LinkedList<String>();
					for (int i = 0; i < doneModel.getSize(); i++) {
						doneList.add(doneModel.get(i).toString());
					}
					doneList.add(CheckSumSQLGenerator.trimTableName(tablename));
					
					//sorting the done list
					Collections.sort(doneList);
					
					//clearing the doneModel to add sorted list 
					doneModel.clear();
					for (String string : doneList) {
						doneModel.addElement(string);
					}
				}
			});
		}
	}

	/**
	 * Update added model in the GUI if the command line is not used.
	 */
	private void updateAddedModelStatusOfTA() {
		if (!commandline) {
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					LinkedList<String> coreList = new LinkedList<String>();
					for (int i = 0; i < addedRepModel.getSize(); i++) {
						if (DisplayTableFrame.compareType.equalsIgnoreCase(CommonCompareUtil.CORETA)) {
							if (addedRepModel.get(i).toString().equalsIgnoreCase(CheckSumSQLGenerator.trimTableName(tablename)+CheckSumSQLGenerator.EXECUTINGCORE)) {
								coreList.add(addedRepModel.get(i).toString().replace(CheckSumSQLGenerator.EXECUTINGCORE, CheckSumSQLGenerator.DONECORE));
							} else {
								coreList.add(addedRepModel.get(i).toString());
							}
						} else if(DisplayTableFrame.compareType.equalsIgnoreCase(CommonCompareUtil.COREGP)){
							if (addedRepModel.get(i).toString().equalsIgnoreCase(CheckSumSQLGenerator.trimTableName(tablename)+CheckSumSQLGenerator.EXECUTINGCORE)) {
								coreList.add(addedRepModel.get(i).toString().replace(CheckSumSQLGenerator.EXECUTINGCORE, CheckSumSQLGenerator.DONECORE));
							} else {
								coreList.add(addedRepModel.get(i).toString());
							}
						} else {
							if (addedRepModel.get(i).toString().equalsIgnoreCase(CheckSumSQLGenerator.trimTableName(tablename)+CheckSumSQLGenerator.EXECUTINGTA)) {
								coreList.add(addedRepModel.get(i).toString().replace(CheckSumSQLGenerator.EXECUTINGTA, CheckSumSQLGenerator.DONETA));
							} else {
								coreList.add(addedRepModel.get(i).toString());
							}
						}
						
					}
					Collections.sort(coreList);
					addedRepModel.clear();
					for (String string : coreList) {
						addedRepModel.addElement(string);
					}
				}
			});
		}
	}

	/**
	 * Update added model in the GUI if the command line is not used.
	 */
	private void updateAddedModelStatusOfCORE() {
		if (!commandline) {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					LinkedList<String> coreList = new LinkedList<String>();
					for (int i = 0; i < addedRepModel.getSize(); i++) {
						
						//checking for comparison type to trim the tables accordingly
						if (DisplayTableFrame.compareType.equalsIgnoreCase(CommonCompareUtil.CORETA)) {
							if (addedRepModel.get(i).toString().equalsIgnoreCase(CheckSumSQLGenerator.trimTableName(tablename))) {
								coreList.add(addedRepModel.get(i)+CheckSumSQLGenerator.EXECUTINGCORE);
							} else {
								coreList.add(addedRepModel.get(i).toString());
							}
						} else if (DisplayTableFrame.compareType.equalsIgnoreCase(CommonCompareUtil.COREGP)){
							if (addedRepModel.get(i).toString().equalsIgnoreCase(CheckSumSQLGenerator.trimTableName(tablename))) {
								coreList.add(addedRepModel.get(i)+CheckSumSQLGenerator.EXECUTINGCORE);
							} else {
								coreList.add(addedRepModel.get(i).toString());
							}
						} else {
							if (addedRepModel.get(i).toString().equalsIgnoreCase(CheckSumSQLGenerator.trimTableName(tablename))) {
								coreList.add(addedRepModel.get(i)+CheckSumSQLGenerator.EXECUTINGTA);
							} else {
								coreList.add(addedRepModel.get(i).toString());
							}
						}
						
					}
					Collections.sort(coreList);
					addedRepModel.clear();
					for (String string : coreList) {
						addedRepModel.addElement(string);
					}
				}
			});
		}
		
	}
}
