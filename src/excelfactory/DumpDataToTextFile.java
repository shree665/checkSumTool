/**
 * 
 */
package excelfactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import persistence.DelayObject;
import persistence.SumObject;
import tools.CommonCompareUtil;

/**
 * @author vivek.subedi
 *
 */
public class DumpDataToTextFile {
	
	private static Logger logger = Logger.getLogger(DumpDataToTextFile.class); 
	private static File file;
	private static String compareType;
	
	public static synchronized void writeIntoFile(File saveFile, Vector<DelayObject> tempVector, String compare) {
		file = saveFile;
		compareType = compare;
		for (DelayObject delayObject : tempVector) {
			Map<String, Map<String, SumObject>> leftSumObject= delayObject.getCoreSideSumObjectMap();
			Map<String, Map<String, SumObject>> rightSumObject = delayObject.getTaSideSumObjectMap();
			Iterator<Entry<String, Map<String, SumObject>>> iterator = leftSumObject.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, Map<String, SumObject>> entryMap = iterator.next();
				String	tablename = entryMap.getKey();
				Map<String, SumObject> leftColumnSumMap = entryMap.getValue();
				Map<String, SumObject> rightColumnSumMap = null;
				
				if (!rightSumObject.isEmpty() || rightSumObject != null) {
					rightColumnSumMap = rightSumObject.get(tablename);
				}
				dataSummaryParse(tablename, leftColumnSumMap,rightColumnSumMap);
			}
		}
	}
	
	/**
	 * @param tablename
	 * @param columnSumMap
	 * @param rightColumnSumMap 
	 */
	private static void dataSummaryParse(String tablename, Map<String, SumObject> leftColumnSumMap, Map<String, SumObject> rightColumnSumMap) {
		try {
			leftColumnSumMap.remove("COMMIT_DTM");
		} catch (Exception e) {
			logger.info("Some Column is missing in resultsets!");
			logger.info(CommonCompareUtil.getStackTrace(e));
		}
		try {
			if (!file.exists()) {
				file.createNewFile();
				logger.info("creating new file");
			}
			
			FileWriter fileWriter = new FileWriter(file, true);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			String header = "";
			if (compareType.equalsIgnoreCase(CommonCompareUtil.CORETA)) {
				header += "TABLE NAME	\t\tCOLUMN NAME	\t\tCORE VALUE	\t\tTOTAL ACCESS VALUE	\n";
			} else if (compareType.equalsIgnoreCase(CommonCompareUtil.COREGP)) {
				header += "TABLE NAME	\t\tCOLUMN NAME	\t\tCORE VALUE	\t\tGREENPLUM	VALUE	\n";
			} else {
				header += "TABLE NAME	\t\tCOLUMN NAME	\t\tTOTAL ACCESS VALUE	\t\tGREENPLUM VALUE	\n";
			}
			
			if (file.length() == 0) {
				fileWriter.append(header);
			}
			
			Iterator<Entry<String, SumObject>> sumIterator = leftColumnSumMap.entrySet().iterator();
			while (sumIterator.hasNext()) {
				Entry<String, SumObject> entrySumMap = sumIterator.next();
				String	columnname = entrySumMap.getKey();
				SumObject leftSumObject = entrySumMap.getValue();
				Object leftSum = leftSumObject.getColumnValue();
				if (leftSum == null) {
					leftSum = "null";
				}
				SumObject rightSumObject = rightColumnSumMap.get(columnname);
				Object rightSum = rightSumObject.getColumnValue();
				if (rightSum == null) {
					rightSum = "null";
				}
				String context = createRow(tablename, columnname, leftSum, rightSum);
				fileWriter.append(context);
			} 
			fileWriter.append("\n");
			bufferedWriter.close();
		}catch (IOException e) {
			
		}
		
	}

	/**
	 * @param tablename
	 * @param columnname
	 * @param leftSum
	 * @param rightSum
	 * @return String
	 */
	private static String createRow(String tablename, String columnname, Object leftSum, Object rightSum) {
		String context = tablename +"\t\t\t"+columnname + "\t\t\t"+leftSum.toString()+"\t\t\t"+rightSum+"\t\t\n";
		
		return context;
	}
}
