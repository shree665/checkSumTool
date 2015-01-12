/**
 * 
 */
package concurrency;

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
public class WorkerThread implements Runnable{
	private static Logger logger = Logger.getLogger(WorkerThread.class);
	private String tableName;
	private LinkedList<String> sumColumns;
	private Vector<DelayObject> allQueryResultVector;
	private AppParameters parameters;
	private DefaultListModel<Object> addedRepModel; 
	private Boolean commandline;
	
	public WorkerThread(String tableName, AppParameters parameters, LinkedList<String> sumColumns, Vector<DelayObject> allQueryResultVector,  DefaultListModel<Object> addedRepModel,
			Boolean commandline) {
		super();
		this.tableName = tableName;
		this.parameters = parameters;
		this.sumColumns = sumColumns;
		this.allQueryResultVector = allQueryResultVector;
		this.addedRepModel = addedRepModel;
		this.commandline = commandline;
	}

	@Override
	public void run() {
		CoreTAService coreTAService = new CoreTAService(parameters.getDb2Username(), parameters.getDb2Password().replaceAll("'", ""), parameters.getRegion());
		GPService gpService = new GPService(parameters.getDb2Username(), parameters.getDb2Password().replaceAll("'", ""), parameters.getGpUsername(), 
				parameters.getGpPassword().replaceAll("'", ""), parameters.getRegion());
		DelayObject delayObject = new DelayObject(tableName);
		Map<String, SumObject> taObject = null;
		Map<String, SumObject> gpObject = null;
		
		if (!commandline) {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					LinkedList<String> coreList = new LinkedList<String>();
					for (int i = 0; i < addedRepModel.getSize(); i++) {
						//checking for comparison type to trim the tables accordingly
						if (addedRepModel.get(i).toString().equalsIgnoreCase(tableName)) {
							coreList.add(addedRepModel.get(i)+CheckSumSQLGenerator.EXECUTING);
						} else {
							coreList.add(addedRepModel.get(i).toString());
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
		try {
			taObject = coreTAService.getTASumforArchive(parameters.getRegion(), tableName, sumColumns);
		}  catch (NullPointerException e) {
			logger.info("["+tableName+"] table query didn't return anything. Please Check your permission on ["+tableName+"] table and its query query.");
			CommonCompareUtil.setErrorTables(tableName);
			logger.debug(CommonCompareUtil.getStackTrace(e));
		} catch (SQLException e) {
			logger.info("["+tableName+"] table query didn't return anything");
			CommonCompareUtil.setErrorTables(tableName);
			logger.debug(CommonCompareUtil.getStackTrace(e));
		} catch (Exception e) {
			logger.info("["+tableName+"] table Query can't run in the database you chose because of ["+e.getMessage()+"] ");
			CommonCompareUtil.setErrorTables(tableName);
			logger.info(CommonCompareUtil.getStackTrace(e));
		}
		
		Map<String, Map<String, SumObject>> taMap = new HashMap<String, Map<String,SumObject>>();
		taMap.put(tableName, taObject);
		delayObject.setCoreSideSumObjectMap(taMap);
		logger.debug("Inserting Table "+delayObject.getTablename()+" into the queue for comparison...");
		
		try {
			gpObject = gpService.getGPArchive(parameters.getRegion(), tableName, sumColumns);
		} catch (NullPointerException e) {
			logger.info("["+tableName+"] table query didn't return anything. Please Check your permission on ["+tableName+"] table and its query query.");
			CommonCompareUtil.setErrorTables(tableName);
			logger.debug(CommonCompareUtil.getStackTrace(e));
		} catch (SQLException e) {
			logger.info("["+tableName+"] table query didn't return anything");
			CommonCompareUtil.setErrorTables(tableName);
			logger.debug(CommonCompareUtil.getStackTrace(e));
		} catch (Exception e) {
			logger.info("["+tableName+"] table Query can't run in the database you chose because of ["+e.getMessage()+"] ");
			CommonCompareUtil.setErrorTables(tableName);
			logger.info(CommonCompareUtil.getStackTrace(e));
		}
		
		Map<String, Map<String, SumObject>> gpMap = new HashMap<String, Map<String,SumObject>>();
		gpMap.put(tableName, gpObject);
		delayObject.setTaSideSumObjectMap(gpMap);
		allQueryResultVector.add(delayObject);
		
		//updating status of tables that are being processed and being processed in core or ta
		if (!commandline) {
			SwingUtilities.invokeLater(new Runnable() {
				@Override
				public void run() {
					LinkedList<String> coreList = new LinkedList<String>();
					for (int i = 0; i < addedRepModel.getSize(); i++) {
						if (addedRepModel.get(i).toString().equalsIgnoreCase(CheckSumSQLGenerator.trimTableName(tableName)+CheckSumSQLGenerator.EXECUTING)) {
							coreList.add(addedRepModel.get(i).toString().replace(CheckSumSQLGenerator.EXECUTING, CheckSumSQLGenerator.DONE));
						} else {
							coreList.add(addedRepModel.get(i).toString());
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
