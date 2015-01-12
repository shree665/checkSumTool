/**
 * 
 */
package excelfactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import persistence.DifferenceObject;
import tools.CommonCompareUtil;
import enums.DifferenceType;

/**
 * Class to create excel reports. This class design excel and writes all the data into the user defined excel file. 
 * Excel report have two tables called All Result Summary and Difference Only. All Result Summary tab holds all the results even though they are same or different
 * Difference only tab only holds the different and missing data. Each tables are seperated by a line so that user can easily read it.
 * 
 * @author vivek.subedi
 *
 */
public class ExcelReportFactory {
	
	private static Logger logger = Logger.getLogger(ExcelReportFactory.class);
	
	public static final Integer MAX_COLUMNS	= 12;
	
	private static Workbook wb;
	private static Row dataRow;
	private static Sheet sumDifferenceSheet;
	private static Sheet onlyDiffernceSheet;
	private static Map<String, CellStyle> styles;
	private static Integer differenceRow;
	private static Integer onlyDifferenceRow;
	private static Integer columnNumber;
	
	public static Map<String, CellStyle> initializeStyles(){
		//Header
		Map<String, CellStyle> styles = new HashMap<String, CellStyle>();
		
		CellStyle style;
		Font titleFont = wb.createFont();
		titleFont.setFontHeightInPoints((short)12);
		titleFont.setColor(IndexedColors.DARK_BLUE.getIndex());
		titleFont.setFontName("Sarif");
		style = (CellStyle) wb.createCellStyle();
		style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);
		style.setFont(titleFont);
		style.setBorderBottom(CellStyle.BORDER_DOUBLE);
		style.setFillForegroundColor(HSSFColor.GREY_25_PERCENT.index);
		style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		styles.put("HEADER", style);
		
		//value
		CellStyle valueStyle;
		titleFont = wb.createFont();
		titleFont.setItalic(true);
		valueStyle = wb.createCellStyle();
		valueStyle.setFont(titleFont);
		valueStyle.setBorderRight(CellStyle.BORDER_THIN);
		styles.put("VALUE", valueStyle);
		
		//MISSING
		style = wb.createCellStyle();
		style.setFillForegroundColor(HSSFColor.GREY_50_PERCENT.index);
		style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		style.setBorderRight(CellStyle.BORDER_THIN);
		styles.put("MISSING", style);
		
		//DIFFERENT
		style = (XSSFCellStyle) wb.createCellStyle();
		style.cloneStyleFrom(valueStyle);
		titleFont = wb.createFont();
		titleFont.setItalic(true);
		titleFont.setColor(HSSFColor.DARK_RED.index);
		style.setFont(titleFont);
		style.setBorderRight(CellStyle.BORDER_THIN);
		styles.put(DifferenceType.DIFFERENT.toString(), style);
		
		//SAME
		style = (XSSFCellStyle) wb.createCellStyle();
		style.cloneStyleFrom(valueStyle);
		titleFont = wb.createFont();
		titleFont.setItalic(true);
		titleFont.setColor(HSSFColor.DARK_GREEN.index);
		style.setFont(titleFont);
		style.setBorderRight(CellStyle.BORDER_THIN);
		styles.put(DifferenceType.SAME.toString(), style);
		
		/** -------------------------- Difference Matching Colors --------------**/
		style = (XSSFCellStyle) wb.createCellStyle();
		titleFont = wb.createFont();
		titleFont.setColor(HSSFColor.WHITE.index);
		style.setFont(titleFont);
		style.setFillForegroundColor(HSSFColor.DARK_RED.index);
		style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		style.setBorderRight(CellStyle.BORDER_THIN);
		styles.put(DifferenceType.DIFFERENT.toString() + " match", style);
		
		style = (XSSFCellStyle) wb.createCellStyle();
		titleFont = wb.createFont();
		titleFont.setColor(HSSFColor.WHITE.index);
		style.setFont(titleFont);
		style.setFillForegroundColor(HSSFColor.DARK_GREEN.index);
		style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		style.setBorderRight(CellStyle.BORDER_THIN);
		styles.put(DifferenceType.SAME.toString() + " match", style);
		
		
		/** -------------------------- No Style or Separator Style --------------**/
		//NONE
		style = (XSSFCellStyle) wb.createCellStyle();
		style.setBorderRight(CellStyle.BORDER_THIN);
		styles.put("NONE", style);
		//BLANK
		style = (XSSFCellStyle) wb.createCellStyle();
		style.setFillForegroundColor(HSSFColor.GREY_25_PERCENT.index);
		style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		styles.put("BLANK", style);
		
		return styles;
	}
	
	private static void initialize() {
		wb = new XSSFWorkbook();
		sumDifferenceSheet = wb.createSheet("All Result Summary");
		onlyDiffernceSheet = wb.createSheet("Difference Only");
		differenceRow = 0;
		onlyDifferenceRow = 0;
	}
	
	public static void generateReport(Map<String, LinkedList<DifferenceObject>> differencesMap, File saveFile, String compareType) {
		initialize();
		
		styles = initializeStyles();
		
		sumDifferenceSheet.setAutobreaks(true);
		sumDifferenceSheet.getPrintSetup().setLandscape(true);
		sumDifferenceSheet.setFitToPage(true);
		
		onlyDiffernceSheet.setAutobreaks(true);
		onlyDiffernceSheet.getPrintSetup().setLandscape(true);
		onlyDiffernceSheet.setFitToPage(true);
		
		
		//create header row on SumDifference Sheet
		dataRow = sumDifferenceSheet.createRow(differenceRow++);
		if (compareType.equalsIgnoreCase(CommonCompareUtil.CORETA)) {
			createReportHeader("TABLE NAME	","","COLUMN NAME	","","CORE VALUE	","","TOTAL ACCESS VALUE	","","DIFFERENT TYPE	","","OFF BY	");
		} else if (compareType.equalsIgnoreCase(CommonCompareUtil.COREGP)) {
			createReportHeader("TABLE NAME	","","COLUMN NAME	","","CORE VALUE	","","GREENPLUM	VALUE	","","DIFFERENT TYPE	","","OFF BY	");
		} else {
			createReportHeader("TABLE NAME	","","COLUMN NAME	","","TOTAL ACCESS VALUE	","","GREENPLUM VALUE	","","DIFFERENT TYPE	","","OFF BY	");
		}
		
		
		//create header row on onlyDifferencesheet
		dataRow = onlyDiffernceSheet.createRow(onlyDifferenceRow++);
		if (compareType.equalsIgnoreCase(CommonCompareUtil.CORETA)) {
			createReportHeader("TABLE NAME	","","COLUMN NAME	","","CORE VALUE	","","TOTAL ACCESS VALUE	","","DIFFERENT TYPE	","","OFF BY	");
		} else if (compareType.equalsIgnoreCase(CommonCompareUtil.COREGP)) {
			createReportHeader("TABLE NAME	","","COLUMN NAME	","","CORE VALUE	","","GREENPLUM	VALUE	","","DIFFERENT TYPE	","","OFF BY	");
		} else {
			createReportHeader("TABLE NAME	","","COLUMN NAME	","","TOTAL ACCESS VALUE	","","GREENPLUM VALUE	","","DIFFERENT TYPE	","","OFF BY	");
		}
		
		//looping through all the data
		Iterator<Entry<String, LinkedList<DifferenceObject>>> reportIterator = differencesMap.entrySet().iterator();
		while (reportIterator.hasNext()) {
			Entry<String, LinkedList<DifferenceObject>> entryMap = reportIterator.next();
			String tableName = entryMap.getKey();
			LinkedList<DifferenceObject>  differenceObjectsList = entryMap.getValue();
			sumDataDifferenceParse(tableName, differenceObjectsList);
			
			//creats the blank row to seperate the tables
			Row blankRow = sumDifferenceSheet.createRow(differenceRow++);
			for(int i = 0; i < MAX_COLUMNS; i++){
				Cell blankCell = blankRow.createCell(i);
				blankCell.setCellStyle(styles.get("BLANK"));
				blankCell.setCellValue("");
			}
			
			Row blankRow2 = onlyDiffernceSheet.createRow(onlyDifferenceRow++);
			for(int i = 0; i < MAX_COLUMNS; i++){
				Cell blankCell2 = blankRow2.createCell(i);
				blankCell2.setCellStyle(styles.get("BLANK"));
				blankCell2.setCellValue("");
			}
		}
		
		
		
		//for sumDifference Sheet
		for (int i = 0; i < MAX_COLUMNS; i++) {
			sumDifferenceSheet.autoSizeColumn(i, true);
		}
		sumDifferenceSheet.setColumnBreak(columnNumber);
		
		//for onlyDifferencesheet
		for (int i = 0; i < MAX_COLUMNS; i++) {
			onlyDiffernceSheet.autoSizeColumn(i, true);
		}
		onlyDiffernceSheet.setColumnBreak(columnNumber);
		
		//writing into the file
		try {
			FileOutputStream outputStream = new FileOutputStream(saveFile);
			wb.write(outputStream);
			outputStream.close();
		} catch (FileNotFoundException e) {
			logger.debug(saveFile.getAbsolutePath() +" file does not exists.");
		} catch (IOException e) {
			logger.debug("Couldn't write into the "+ saveFile.getAbsolutePath()+" file.");
		}

	}

	//Paring data and creating row of the excel sheet using data
	private static void sumDataDifferenceParse(String tableName, LinkedList<DifferenceObject> differenceObjectsList) {
		
		for (DifferenceObject differenceObject : differenceObjectsList) {
			if (differenceObject != null) {
				String columnName = differenceObject.getColumnName();
				Object coreValue = differenceObject.getCoreColumnValue();
				Object taValue = differenceObject.getTaColumnValue();
				Object differBy = differenceObject.getDifferBy();
				DifferenceType differenceType = differenceObject.getDifferenceType();
				dataRow = sumDifferenceSheet.createRow(differenceRow++);
				createRow(tableName, columnName, coreValue, taValue, differenceType, differBy);
				
				if (differenceType.equals(DifferenceType.DIFFERENT) || differenceType.equals(DifferenceType.MISSING)) {
					dataRow = onlyDiffernceSheet.createRow(onlyDifferenceRow++);
					createRow(tableName, columnName, coreValue, taValue, differenceType, differBy);
				}
			} else {
				logger.debug("["+tableName+"] can't be processed");
				continue;
			}
			
		}
	}


	//Creating Cell for each value for a row
	private static void createRow(String tableName, String columnName, Object coreValue, Object taValue, DifferenceType differenceType, Object differByObject) {
		String difference = differenceType.toString();
		columnNumber = 0;
		
		Cell dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellValue(tableName);
		dataCell.setCellStyle(styles.get("VALUE"));
		
		//SEPERATOR
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellStyle(styles.get("BLANK"));
		
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellValue(columnName);
		dataCell.setCellStyle(styles.get("VALUE"));
		
		//SEPERATOR
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellStyle(styles.get("BLANK"));
		
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellValue(coreValue.toString());
		dataCell.setCellStyle(styles.get("VALUE"));
		
		//SEPERATOR
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellStyle(styles.get("BLANK"));
		
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellValue(taValue.toString());
		dataCell.setCellStyle(styles.get("VALUE"));
		
		//SEPERATOR
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellStyle(styles.get("BLANK"));
		
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellValue(difference);
		dataCell.setCellStyle(styles.get(difference + " match"));
		
		//SEPERATOR
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellStyle(styles.get("BLANK"));
		
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellValue(differByObject.toString());
		dataCell.setCellStyle(styles.get("VALUE"));
		
		//SEPERATOR
		dataCell = dataRow.createCell(columnNumber++);
		dataCell.setCellStyle(styles.get("BLANK"));
		
	}

	private static void createReportHeader(String...headerValue ) {
		sumDifferenceSheet.createFreezePane(0, 1);
		onlyDiffernceSheet.createFreezePane(0, 1);
		sumDifferenceSheet.setAutoFilter(CellRangeAddress.valueOf("A1:K1"));
		onlyDiffernceSheet.setAutoFilter(CellRangeAddress.valueOf("A1:K1"));
		Integer index = 0;
		for (String header : headerValue) {
			Cell titleCell = dataRow.createCell(index++);
			titleCell.setCellValue(header);
			titleCell.setCellStyle(styles.get("HEADER"));
		}
		
	}
	
}
