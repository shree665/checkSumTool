/**
 * 
 */
package compare;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import enums.DifferenceType;
import persistence.AppParameters;
import persistence.DifferenceObject;
import persistence.SumObject;
import tools.CommonCompareUtil;


/**
 * This class finds the difference of each tables by comparing the value of CORE, TA and Greenplum resultsets depending on the user's choice.
 * 
 * @author vivek.subedi
 *
 */
public class CompareData {
	
	private static Logger logger = Logger.getLogger(CompareData.class);
	
	/**
	 * Static method to compare data of two maps i.e. form DB2 and Greenplum. Compares each value as a double.
	 *
	 * @param coreTaSumObjects - @Map to hold the column name with the sum of a column either core or ta
	 * @param taGpSumObjects - @Map to hold the column name with sum of column either ta or greenplum
	 * @return the linked list of difference object which holds all the comparisons
	 */
	public static LinkedList<DifferenceObject> compareDataMap(Map<String, SumObject> coreTaSumObjects, Map<String, SumObject> taGpSumObjects, AppParameters parameters){
		
		LinkedList<DifferenceObject> differenceObjectsList = new LinkedList<DifferenceObject>();
		//Following columns are removing from the list because these columns are meant to be different
		//String tableName = (String) coreTaSumObjects.get("TABLE_NAME").getColumnValue();
		try {
			coreTaSumObjects.remove("COMMIT_DTM");
			coreTaSumObjects.remove("DB_SIDE");
			coreTaSumObjects.remove("TABLE_NAME");
		} catch (Exception e) {
			logger.info("Some Column is missing in resultsets!");
			logger.info(CommonCompareUtil.getStackTrace(e));
		}
		
		Iterator<Entry<String, SumObject>> coreIterator = coreTaSumObjects.entrySet().iterator();
		try {
			while (coreIterator.hasNext()) {
				Entry<String, SumObject> entry = coreIterator.next();
				String coreTASumColumn = entry.getKey();
				Object coreTASumValue = entry.getValue().getColumnValue();
				Object taGPSumValue = null;
				if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA)) {
					taGPSumValue = taGpSumObjects.get(coreTASumColumn).getColumnValue();
				} else {
					taGPSumValue = taGpSumObjects.get(coreTASumColumn).getColumnValue();
				}
				
				
				if (coreTASumValue == null && taGPSumValue == null) {
					DifferenceType differenceType = DifferenceType.SAME;
					DifferenceObject differenceObject = createDifferenceObject(coreTASumColumn, coreTASumValue, taGPSumValue, differenceType);
					differenceObject.setDifferBy("Null");
					differenceObjectsList.add(differenceObject);
					continue;
				}
				
				if (coreTASumValue == null && taGPSumValue != null) {
					DifferenceType differenceType = DifferenceType.MISSING;
					DifferenceObject differenceObject = createDifferenceObject(coreTASumColumn, coreTASumValue, taGPSumValue, differenceType);
					if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.TAGP)) {
						differenceObject.setDifferBy("Missing in TA !");
					} else {
						differenceObject.setDifferBy("Missing in Core !");
					}
					
					differenceObjectsList.add(differenceObject);
					continue;
				}
				
				if (coreTASumValue != null && taGPSumValue == null) {
					DifferenceType differenceType = DifferenceType.MISSING;
					DifferenceObject differenceObject = createDifferenceObject(coreTASumColumn, coreTASumValue, taGPSumValue, differenceType);
					if (parameters.getComparisonType().equalsIgnoreCase(CommonCompareUtil.CORETA)) {
						differenceObject.setDifferBy("Missing in Total Access !");
					} else {
						differenceObject.setDifferBy("Missing in Greenplum !");
					}
					
					differenceObjectsList.add(differenceObject);
					continue;
				}
				
				boolean compared = compareValues(coreTASumValue, taGPSumValue);
				
				if (!compared) {
					DifferenceType differenceType = DifferenceType.DIFFERENT;
					DifferenceObject differenceObject = createDifferenceObject(coreTASumColumn, coreTASumValue, taGPSumValue, differenceType);
					Double coreValue = Double.parseDouble(coreTASumValue.toString().trim());
					Double taValue = Double.parseDouble(taGPSumValue.toString().trim());
					differenceObject.setDifferBy(coreValue - taValue);
					differenceObjectsList.add(differenceObject);
				} else {
					DifferenceType differenceType = DifferenceType.SAME;
					DifferenceObject differenceObject = createDifferenceObject(coreTASumColumn, coreTASumValue, taGPSumValue, differenceType);
					differenceObject.setDifferBy(0);
					differenceObjectsList.add(differenceObject);
				}
			}
		} catch (Exception e) {
			logger.info(CommonCompareUtil.getStackTrace(e));
		}
		
		return differenceObjectsList;
	}
	

	/**
	 * Creates the difference object.
	 *
	 * @param coreColumnName - the column name
	 * @param coreSumValue - The sum value of a column
	 * @param taSumValue - The sum value of a column
	 * @param differenceType - The type of difference i.e missing, same and different
	 * @return the difference object
	 */
	private static DifferenceObject createDifferenceObject(String coreColumnName, Object coreSumValue, Object taSumValue, DifferenceType differenceType) {
		
		DifferenceObject differenceObject = new DifferenceObject();
		differenceObject.setColumnName(coreColumnName);
		
		/**
		 * Checking for null values in core and ta columns. If the value is null, then we replace it with string "null" to avoid the null pointer exception 
		 * while writing into the excel file
		 */

		if (coreSumValue == null) {
			differenceObject.setCoreColumnValue("Null");
		} else {
			differenceObject.setCoreColumnValue(coreSumValue);
		}
		
		if (taSumValue == null) {
			differenceObject.setTaColumnValue("Null");
		} else {
			differenceObject.setTaColumnValue(taSumValue);
		}
		differenceObject.setDifferenceType(differenceType);
		
		return differenceObject;
	}


	/**
	 * Compare values of CORE and TA.
	 *
	 * @param coreSumValue - The sum value of a column
	 * @param taSumValue - the sum value of a column
	 * @return true, if successful
	 */
	private static boolean compareValues(Object coreTASumValue, Object taGPSumValue) {
		Double coreValue = Double.parseDouble(coreTASumValue.toString().trim());
		Double taValue = Double.parseDouble(taGPSumValue.toString().trim());
		
		if (coreValue.equals(taValue)) {
			return true;
		}
		return false;
	}
}
