/**
 * 
 */
package tools;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;

import persistence.DatabaseRegion;

/**
 * This is a Util class for DataComarisionTool. All the methods those are used more than once are here. 
 * 
 * @author vivek.subedi
 *
 */
public class CheckSumSQLGenerator {
	
	private static Logger logger = Logger.getLogger(CheckSumSQLGenerator.class);
	
	//variables
	private static String taChs;
	private static LinkedList<String> countTablesList;
	private static LinkedList<String> updatedCountTableList = new LinkedList<String>();
	public static DatabaseRegion databaseRegion;
	
	//Constants
	public static final String EXECUTINGCORE = "  ----  Executing in CORE...";
	public static final String EXECUTINGTA = "  ----  Executing in TA...";
	public static final String DONECORE	 = "  ----  Done in CORE !";
	public static final String DONETA	 = "  ----  Done in TA !";
	public static final String EXECUTING = "  ---- Executing...";
	public static final String DONE = "   ---- Done !";
	public static final String[] TIME = {"1","2","3","4","5"};
	public static final String[] THREADS = {"2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20"};
	public static final String[] TATABLES = {"CRED_TRANS","CRED_DECLN_RSN","CRED_OVERRIDE","CRED_TAX_LIEN_DTL","CRED_PUB_REC","CREDIT_APPEAL"};
	public static final String EDRESPONSE = "ED_RESPONSE";
	public static final String SCHEMAPREFIX = "CHS";
	public static final String ARCHIVESHCEMAPREFIX = "AR";
	public static final String REPSERVERSCHEMA = "OEDCOD";
	public static final String SOURCENAME = " SOURCE_NAME ";
	public static final String PERCENTAGE = "%";
	public static final String STATE = "A";
	public static final String AND = " AND ";
	public static final String DOT = ".";
	public static final String RS = "RS_";
	public static final String SUM = "SUM_";
	public static final String CAST = "CAST(";
	public static final String ASDECIMAL = " AS DECIMAL(31, 0) ";
	public static final String COUNT = "COUNT(*)";
	public static final String SINGLEQUOTE = "'";
	public static final String REGION = "<REGION>";
	public static final String TED = " 'TED^_"+PERCENTAGE + "' ESCAPE '^' ";
	public static final String ASN = "ASNQ";
	public static final String UNIONALL = " UNION ";
	public static final String WITHUR = " WITH UR;";
	public static final String REPSERVER = "IBMQREP_SUBS";
	public static final String SELECT1 = "SELECT "+SOURCENAME+" AS TBNAME FROM ";
	public static final String SUBNAME = " "+AND + " SUBNAME LIKE '%GP%'"; 
	public static final String WHERESOURCE = " WHERE SOURCE_OWNER IN (";
	public static final String ANDCLAUSE = AND+ " SENDQ LIKE "+ SINGLEQUOTE + PERCENTAGE + REGION + PERCENTAGE + SINGLEQUOTE + AND + 
			SOURCENAME + " NOT LIKE " + TED + AND + " STATE = " + SINGLEQUOTE + STATE + SINGLEQUOTE + " AND SENDQ LIKE '%SERVER%'";
	public static final String ANDCLAUSE2 = AND+ " SENDQ LIKE "+ SINGLEQUOTE + PERCENTAGE + REGION + PERCENTAGE + SINGLEQUOTE + AND + 
			SOURCENAME + " LIKE " + TED + AND + " STATE = " + SINGLEQUOTE + STATE + SINGLEQUOTE + " AND SENDQ LIKE '%SERVER%'";
	public static final String SELECT2 = "SELECT SUBSTR("+SOURCENAME+","+2+")"+" AS TBNAME FROM ";
	
	

	/**
	 * Creates Table query using db2Region
	 *
	 * @param db2Region the db2 region
	 * @return Reptable query for core to ta
	 */
	public static String getDB2RepTableQuery(DatabaseRegion db2Region) {
		databaseRegion = db2Region;
		
		String from = ASN + db2Region.getCore() + DOT + REPSERVER + WHERESOURCE + SINGLEQUOTE + SCHEMAPREFIX +
				db2Region.getRegion() + SINGLEQUOTE + "," + SINGLEQUOTE + REPSERVERSCHEMA + SINGLEQUOTE + ") ";
		String from2 = ASN+db2Region.getCore() + DOT + REPSERVER;
		String query = SELECT1 + from + ANDCLAUSE + UNIONALL + SELECT2 + from2 + WHERESOURCE + SINGLEQUOTE + SCHEMAPREFIX +
				db2Region.getRegion() + SINGLEQUOTE + "," + SINGLEQUOTE + REPSERVERSCHEMA + SINGLEQUOTE + ") "+ ANDCLAUSE2;
		query = query.replace("<REGION>", db2Region.getRegion());
		
		return query;
	}
	
	/**
	 * Creates Table query using db2Region
	 *
	 * @param db2Region the db2 region
	 * @return archive query for core to ta
	 */
	public static String getDB2ArchiveTableQuery(DatabaseRegion db2Region) {
		databaseRegion = db2Region;
		
		String query = "SELECT NAME FROM SYSIBM.SYSTABLES WHERE CREATOR = '"+ARCHIVESHCEMAPREFIX+databaseRegion.getRegion()+"' ";
		
		return query;
	}
	
	/**
	 * Creates table query using db2region for greenplum. Creates query to give rep tables from CORE to GREENPLUM
	 *
	 * @param db2Region the db2 region
	 * @return Reptable query for core to greenplum
	 */
	public static String getGreenplumCoreRepTableQuery(DatabaseRegion db2Region) {
		databaseRegion = db2Region;
		String from = ASN + db2Region.getCore() + DOT + REPSERVER + WHERESOURCE + SINGLEQUOTE + SCHEMAPREFIX +
				db2Region.getRegion() + SINGLEQUOTE + "," + SINGLEQUOTE + REPSERVERSCHEMA + SINGLEQUOTE + ") ";
		String from2 = ASN+db2Region.getCore() + DOT + REPSERVER;
		String query = SELECT1 + from + ANDCLAUSE +SUBNAME+ UNIONALL + SELECT2 + from2 + WHERESOURCE + SINGLEQUOTE + SCHEMAPREFIX +
				db2Region.getRegion() + SINGLEQUOTE + "," + SINGLEQUOTE + REPSERVERSCHEMA + SINGLEQUOTE + ") "+ ANDCLAUSE2 + SUBNAME;
		query = query.replace("<REGION>", db2Region.getRegion());
		query = query.replace("AND SENDQ LIKE '%SERVER%'", "");
		
		return query;
	}
	
	/**
	 * Creates table query using db2region for greenplum. Creates query to give rep tables from TOTAL ACCESS to GREENPLUM
	 *
	 * @param db2Region the db2 region
	 * @return rep table query for ta to greenplum
	 */
	public static String getGreenplumTARepTableQuery(DatabaseRegion db2Region) {
		databaseRegion = db2Region;
		String from = ASN + db2Region.getTa() + DOT + REPSERVER + WHERESOURCE + SINGLEQUOTE + SCHEMAPREFIX +
				db2Region.getRegion() + SINGLEQUOTE + "," + SINGLEQUOTE + REPSERVERSCHEMA + SINGLEQUOTE + ") ";
		String from2 = ASN+db2Region.getTa() + DOT + REPSERVER;
		String query = SELECT1 + from + ANDCLAUSE +SUBNAME+ UNIONALL + SELECT2 + from2 + WHERESOURCE + SINGLEQUOTE + SCHEMAPREFIX +
				db2Region.getRegion() + SINGLEQUOTE + "," + SINGLEQUOTE + REPSERVERSCHEMA + SINGLEQUOTE + ") "+ ANDCLAUSE2 + SUBNAME;
		query = query.replace("<REGION>", db2Region.getRegion());
		query = query.replace("AND SENDQ LIKE '%SERVER%'", "");
		
		return query;
	}

	/**
	 * Gets the sum column query the CORE to TA and CORE to GREENPLUM comparasion. It retrives all those columns which are in (bigint, float, integer, smallint and decimal)
	 *
	 * @param db2Region the db2 region
	 * @param selectedTableList - table list
	 * @return the sum column query
	 */
	public static String getSumColumnQuery(DatabaseRegion db2Region, LinkedList<String> selectedTableList) {
		
		String tableSelect = "SELECT (RTRIM(TBCREATOR) || '.' || TBNAME) AS SCHEMA_TABLE, NAME FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR IN (";
		String where = SINGLEQUOTE + SCHEMAPREFIX + db2Region.getRegion() + SINGLEQUOTE + ", '"+ REPSERVERSCHEMA + "')";
		String and = " AND TBNAME IN ( " + getTableNameStringForSumColumns(selectedTableList) + " ) AND COLTYPE IN ('BIGINT', 'FLOAT', 'INTEGER', 'SMALLINT', 'DECIMAL') ";
		
		String query = tableSelect + where + and;
		return query;
		
	}
	

	/**
	 * Creates a query to find the columns that are in (bigint, float, integer, smallint and decimal) using the user selected tables for TOTAL ACCESS TO GREENPLUM
	 *
	 * @param db2Region the db2 region
	 * @param selectedTableList - user selected table list
	 * @return the Total Access sum column query
	 */
	public static String getTASumColumnQuery(DatabaseRegion db2Region, LinkedList<String> selectedTableList) {
		
		String tableSelect = "SELECT (RTRIM(TBCREATOR) || '.' || TBNAME) AS SCHEMA_TABLE, NAME FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR IN (";
		String where = SINGLEQUOTE + SCHEMAPREFIX + db2Region.getRegion() + SINGLEQUOTE + ", '"+ REPSERVERSCHEMA + "')";
		String and = " AND TBNAME IN ( " + getTableNameStringForSumColumnsofTA(selectedTableList) + " ) AND COLTYPE IN ('BIGINT', 'FLOAT', 'INTEGER', 'SMALLINT', 'DECIMAL') ";
		
		String query = tableSelect + where + and;
		return query;
		
	}
	
	/**
	 * Gets the sum column query the CORE to TA and CORE to GREENPLUM comparasion. It retrives all those columns which are in (bigint, float, integer, smallint and decimal)
	 *
	 * @param db2Region the db2 region
	 * @param selectedTableList - table list
	 * @return the sum column query
	 */
	public static String getArchiveSumColumnQuery(DatabaseRegion db2Region, LinkedList<String> selectedTableList) {
		
		String tableSelect = "SELECT TBNAME AS SCHEMA_TABLE, NAME FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR ="+SINGLEQUOTE + ARCHIVESHCEMAPREFIX + 
				db2Region.getRegion()+SINGLEQUOTE;
		String and = " AND TBNAME IN ( " + getTableNameStringForSumColumnsofTA(selectedTableList) + " ) AND COLTYPE IN ('BIGINT', 'FLOAT', 'INTEGER', 'SMALLINT', 'DECIMAL') ";
		
		String query = tableSelect + and;
		return query;
		
	}
	

	private static String getTableNameStringForSumColumnsofTA(LinkedList<String> selectedTableList) {
		String tableNameString = "";
		for (String string : selectedTableList) {
			tableNameString += SINGLEQUOTE + string+SINGLEQUOTE +", ";
		}
		tableNameString = tableNameString.substring(0, tableNameString.length()-2);
		
		return tableNameString;
	}

	
	/**
	 * Gets the primary column query for CORE TO TA Comparison.
	 *
	 * @param db2Region the db2 region
	 * @param selectedTableList - the selected table list
	 * @return the primary column query
	 */
	public static String getPrimaryColumnQuery(DatabaseRegion db2Region, LinkedHashSet<String> selectedTableList) {
		
		return "SELECT SUBSTR(TBNAME, 4) AS TBNAME, SUBSTR(NAME, 3) AS NAME FROM SYSIBM.SYSCOLUMNS WHERE TBCREATOR IN ("+SINGLEQUOTE+SCHEMAPREFIX+db2Region.getRegion()+
				SINGLEQUOTE + ") AND TBNAME IN (" + getTableNamesStringForPrimaryKey(selectedTableList) + ") AND NAME LIKE 'O^_%' ESCAPE '^'";
	}
	

	/**
	 * creates a query to find the primary key columns using the user selected tables for TOTAL ACCESS TO GREENPLUM AND CORE TO GREENPLUM comparison
	 *
	 * @param db2Region - the db2 region
	 * @param selectedTableList - the selected table list
	 * @return the GP primary column query
	 */
	public static String getGPPrimaryColumnQuery(DatabaseRegion db2Region, LinkedHashSet<String> selectedTableList) {
		
		return "select substr(table_name, 4) AS tbname, substr(column_name, 3) AS name FROM information_schema.columns WHERE table_schema = "+SINGLEQUOTE+SCHEMAPREFIX.toLowerCase()+db2Region.getRegion()+
				SINGLEQUOTE + " AND table_name IN (" + getTableNamesStringForPrimaryKey(selectedTableList) + ") AND column_name LIKE 'o^_%' ESCAPE '^'";
	}
	
	
	/**
	 * creates a CORE query to sum the columns of a tables that are in (bigint, float, integer, smallint and decimal) for CORE to TA and CORE to GREENPLUM
	 *
	 * @param db2Region - the db2 region
	 * @param tableName - the table name
	 * @param columnList - the column list
	 * @param hourNeedsTobeAdded - hour that needs to be added because of GMT issue
	 * @return the core sum query
	 */
	public static String getCoreSumQuery(DatabaseRegion db2Region, String tableName, LinkedList<String> columnList, Integer hourNeedsTobeAdded) {
		
		String newTrimmedTableName =trimTableName(tableName);
		
		if (updatedCountTableList != null || !updatedCountTableList.isEmpty()) {
			if (updatedCountTableList.contains(newTrimmedTableName)) {
				String query =   "SELECT (CURRENT TIMESTAMP + "+hourNeedsTobeAdded + " HOURS) AS COMMIT_DTM, "+SINGLEQUOTE + newTrimmedTableName + SINGLEQUOTE + " AS TABLE_NAME, 'CORE' AS DB_SIDE, "
						+ COUNT + " AS COUNT_"+newTrimmedTableName + " FROM " + tableName;
				return query;
			} else {
				String query =  "SELECT(CURRENT TIMESTAMP + "+hourNeedsTobeAdded + " HOURS) AS COMMIT_DTM, "+SINGLEQUOTE + newTrimmedTableName + SINGLEQUOTE + " AS TABLE_NAME, 'CORE' AS DB_SIDE, "
						+getCoreSideSumColumnString(columnList) + COUNT + " AS COUNT_"+newTrimmedTableName + " FROM " + tableName;
				return query;
			}
		} else {
			String query =   "SELECT (CURRENT TIMESTAMP + "+hourNeedsTobeAdded + " HOURS) AS COMMIT_DTM, "+SINGLEQUOTE + trimTableName(tableName) + SINGLEQUOTE + " AS TABLE_NAME, 'CORE' AS DB_SIDE, "
					+getCoreSideSumColumnString(columnList) + COUNT + " AS COUNT_"+newTrimmedTableName + " FROM " + tableName;
			return query;
		}
		
	}
	
	
	/**
	 * creates TOTAL Access query to sum columns of a table that are in (bigint, float, integer, smalling and decmal) for TA to Greenplum comparison
	 *
	 * @param region - the region
	 * @param tableName - the table name
	 * @param columnList - the column list
	 * @param hourNeedsTobeAdded - the hour needs tobe added for GMT but we are not using it since we are doing straight sum of TA and GP tables
	 * @return the Total Acces sum query
	 */
	public static String getGPTASumQuery(DatabaseRegion region, String tableName, LinkedList<String> columnList, Integer hourNeedsTobeAdded) {
		String newTrimmedTableName = trimTableName(tableName);;
		
		if (updatedCountTableList != null || !updatedCountTableList.isEmpty()) {
			if (updatedCountTableList.contains(newTrimmedTableName)) {
				String query =   "SELECT CURRENT TIMESTAMP AS COMMIT_DTM, "+SINGLEQUOTE + newTrimmedTableName + SINGLEQUOTE + " AS TABLE_NAME, 'TOTAL ACCESS'  AS DB_SIDE, "
						+ COUNT + " AS COUNT_"+newTrimmedTableName + " FROM " + tableName;
				return query;
			} else {
				String query =  "SELECT CURRENT TIMESTAMP AS COMMIT_DTM, "+SINGLEQUOTE + newTrimmedTableName + SINGLEQUOTE + " AS TABLE_NAME, 'TOTAL ACCESS'  AS DB_SIDE, "
						+getCoreSideSumColumnString(columnList) + COUNT + " AS COUNT_"+newTrimmedTableName + " FROM " + tableName;
				return query;
			}
		} else {
			String query =   "SELECT CURRENT TIMESTAMP AS COMMIT_DTM, "+SINGLEQUOTE + trimTableName(tableName) + SINGLEQUOTE + " AS TABLE_NAME, 'TOTAL ACCESS' AS DB_SIDE, "
					+getCoreSideSumColumnString(columnList) + COUNT + " AS COUNT_"+trimTableName(tableName) + " FROM " + tableName;
			return query;
		}
	}
	

	/**
	 * creates TOTAL Access query to sum columns fo a table that are in (bigint, float, integer, smalling and decmal) for archive comparison
	 *
	 * @param region - the region
	 * @param tableName - the table name
	 * @param columnList - the column list
	 * @param hourNeedsTobeAdded - the hour needs tobe added for GMT but we are not using it since we are doing straight sum of TA and GP tables
	 * @return the Total Acces sum query
	 */
	public static String getArchiveTASumQuery(DatabaseRegion region, String tableName, LinkedList<String> columnList ) {
		
		if (countTablesList != null || !countTablesList.isEmpty()) {
			if (countTablesList.contains(tableName)) {
				String query =   "SELECT CURRENT TIMESTAMP AS COMMIT_DTM, "+SINGLEQUOTE + tableName + SINGLEQUOTE + " AS TABLE_NAME, 'TOTAL ACCESS'  AS DB_SIDE, "
						+ COUNT + " AS COUNT_"+tableName + " FROM " +ARCHIVESHCEMAPREFIX+region.getRegion()+DOT+ tableName;
				return query;
			} else {
				String query =  "SELECT CURRENT TIMESTAMP AS COMMIT_DTM, "+SINGLEQUOTE + tableName + SINGLEQUOTE + " AS TABLE_NAME, 'TOTAL ACCESS'  AS DB_SIDE, "
						+getCoreSideSumColumnString(columnList) + COUNT + " AS COUNT_"+tableName + " FROM " + ARCHIVESHCEMAPREFIX+region.getRegion()+DOT+ tableName;
				return query;
			}
		} else {
			String query =   "SELECT CURRENT TIMESTAMP AS COMMIT_DTM, "+SINGLEQUOTE + trimTableName(tableName) + SINGLEQUOTE + " AS TABLE_NAME, 'TOTAL ACCESS' AS DB_SIDE, "
					+getCoreSideSumColumnString(columnList) + COUNT + " AS COUNT_"+trimTableName(tableName) + " FROM " +ARCHIVESHCEMAPREFIX+region.getRegion()+DOT+ tableName;
			return query;
		}
	}
	

	/**
	 * Gets the table name with schema for count table.
	 *
	 * @param db2Region - the db2 region
	 * @param noSumColumnsTables - the list of tables that don't have sum columns
	 * @return Query to find the schema of tables
	 */
	public static String getTableNameWithSchemaforCountTable(DatabaseRegion db2Region, LinkedList<String> noSumColumnsTables) {
		countTablesList = noSumColumnsTables;
		
		for (String string : countTablesList) {
			if (string.startsWith("TED_")) {
				updatedCountTableList.add(string.replace("TED_", "ED_"));
			} else {
				updatedCountTableList.add(string);
			}
		}
		
		return "SELECT (RTRIM(CREATOR) || '.' || NAME) AS SCHEMA_TABLE FROM SYSIBM.SYSTABLES WHERE CREATOR IN ("+ SINGLEQUOTE+SCHEMAPREFIX+db2Region.getRegion()+SINGLEQUOTE+", '"+
				REPSERVERSCHEMA+"')"+ AND + " NAME IN ("+getColumnsForCount(noSumColumnsTables)+")";
	}
	

	/**
	 * Creates TA Sum query for those tables which has sum columns
	 *
	 * @param db2Region - the db2 region
	 * @param tableName - the table name
	 * @param primaryColumnList - the primary column list of a table
	 * @param sumColumns - the sum columns of a table
	 * @param object - Object of commit_dtm to get all records those are after inserted that time in RS_* table
	 * @return Query to execute in TA
	 */
	public static String getTaSumQuery(DatabaseRegion db2Region, String tableName, LinkedList<String> primaryColumnList, LinkedList<String> sumColumns, Object object) {
		
		Set<String> linkedHashSet = new LinkedHashSet<String>();
		for (String string : sumColumns) {
			linkedHashSet.add(string);
		}
		logger.debug("Table name with schema name: "+tableName);
		logger.debug("Table Name without schema: "+trimTableName(tableName));
		for (String string : primaryColumnList) {
			linkedHashSet.add(string);
		}
		
		LinkedList<String> taTablesList = new LinkedList<String>();
		for (int i = 0; i < TATABLES.length; i++) {
			taTablesList.add(TATABLES[i]);
		}
		
		//Checks for the schema prefix. Some tables use TA schemas
		if (taTablesList.contains(trimTableName(tableName))) {
			taChs = "TA";
			logger.debug(trimTableName(tableName) + " uses TA schema.");
		} else {
			taChs = SCHEMAPREFIX;
			logger.debug(trimTableName(tableName) + " uses CHS schema.");
		} 
		
		String combinedTASumQuery = "";
		
		//checking for the ED_RESPONSE
		if (!trimTableName(tableName).equalsIgnoreCase("ED_RESPONSE")) {
			String tempQuery = getTaWithString(tableName, linkedHashSet) + " AS ( SELECT " + selectT1(linkedHashSet) + " FROM "+SCHEMAPREFIX+db2Region.getRegion()+DOT+"RS_"
					+trimTableName(tableName) + " t1, (" + innerSelect(primaryColumnList, object, db2Region, tableName) + "t1.RS_CREATE_DTM = t2.RS_CREATE_DTM AND "
					+tempWhereClause(primaryColumnList) + ") ";
			String mainSelectStatement = " SELECT "+SINGLEQUOTE+trimTableName(tableName) + SINGLEQUOTE+" AS TABLE_NAME, "+SINGLEQUOTE+"TOTAL_ACCESS"+SINGLEQUOTE
					+" AS DB_SIDE "+ tempSumColumns(sumColumns) + ", SUM(COUNT_"+trimTableName(tableName)+") AS COUNT_"+trimTableName(tableName) + " FROM (";
			
			String firstInnerSelect = " SELECT "+createFirstInnerSelect(sumColumns) + COUNT + " AS COUNT_"+trimTableName(tableName)+ " FROM "+taChs
					+db2Region.getRegion()+DOT+trimTableName(tableName)+ " TA ";
			
			String secondInnerSelect = " SELECT "+createSecondInnerSelect(sumColumns) + COUNT + " * -1 AS COUNT_"+trimTableName(tableName)+ " FROM "+ taChs
					+db2Region.getRegion()+DOT+trimTableName(tableName)+ " INNERTA ";
			
			String innerJoin = " INNER JOIN LATEST_RS_"+trimTableName(tableName) + " INNERRS ON " + tempWhereClause(primaryColumnList).replaceAll("t1", "INNERTA");
			innerJoin = innerJoin.replaceAll("t2", "INNERRS");
			
			String thirdInnerSelect = " SELECT "+getThirdInnerSelect(sumColumns, tableName) +" FROM LATEST_RS_"+trimTableName(tableName) + " WHERE ACTION_TYP IN (0, 1)";
			
			combinedTASumQuery += tempQuery + mainSelectStatement + firstInnerSelect + " UNION ALL "+ secondInnerSelect + innerJoin + " UNION ALL" + 
					thirdInnerSelect + " ) AS FULL_"+trimTableName(tableName);
		}
		else {
			logger.debug("The table is ED_RESPONSE. We are using special method to construct query for this table.");
			String tempQuery = getTaWithString(tableName, linkedHashSet) + " AS ( SELECT " + selectT1(linkedHashSet) + " FROM "+SCHEMAPREFIX+db2Region.getRegion()+DOT+"RS_"
					+trimTableName(tableName) + " t1 WHERE COMMIT_DTM <= " + SINGLEQUOTE + object + SINGLEQUOTE +")";
			
			String mainSelectStatement = " SELECT "+SINGLEQUOTE+trimTableName(tableName) + SINGLEQUOTE+" AS TABLE_NAME, "+SINGLEQUOTE+"TOTAL_ACCESS"+SINGLEQUOTE
					+" AS DB_SIDE "+ tempSumColumns(sumColumns) + ", SUM(COUNT_"+trimTableName(tableName)+") AS COUNT_"+trimTableName(tableName) + " FROM (";
			
			String firstInnerSelect = " SELECT "+createFirstInnerSelect(sumColumns) + COUNT + " AS COUNT_"+trimTableName(tableName)+ " FROM "+taChs
					+db2Region.getRegion()+DOT+trimTableName(tableName)+ " TA ";
			
			String thirdInnerSelect = " SELECT "+getThirdInnerSelect(sumColumns, tableName) +" FROM LATEST_RS_"+trimTableName(tableName) + " WHERE ACTION_TYP IN (0, 1)";
			
			combinedTASumQuery += tempQuery + mainSelectStatement + firstInnerSelect + " UNION ALL " + thirdInnerSelect + ") AS FULL_"+trimTableName(tableName);
		} 

		return combinedTASumQuery;
	}
	
	
	/**
	 * Query for Greenplum and Core Comparison
	 *
	 * @param db2Region - theregion
	 * @param tableName - the table name
	 * @param primaryColumnList - the primary column list
	 * @param sumColumns - the sum columns
	 * @param object - Object of commit_dtm to get all records those are after inserted that time in RS_* table
	 * @return The Query to execute in Greenplum
	 */
	public static String getCOREGPQuery(DatabaseRegion db2Region, String tableName, LinkedList<String> primaryColumnList, LinkedList<String> sumColumns, Object object) {
		
		Set<String> linkedHashSet = new LinkedHashSet<String>();
		
		for (String string : sumColumns) {
			linkedHashSet.add(string);
		}
		
		logger.debug("Table name with schema name: "+tableName);
		logger.debug("Table Name without schema: "+trimTableName(tableName));
		for (String string : primaryColumnList) {
			linkedHashSet.add(string);
		}
		
		String combinedTASumQuery = "";
		
	
		String tempQuery = getTaWithString(tableName, linkedHashSet) + " AS ( SELECT " + selectT1(linkedHashSet) + " FROM "+SCHEMAPREFIX+db2Region.getRegion()+DOT+"RS_"
				+trimTableName(tableName) + " t1, (" + innerSelect(primaryColumnList, object, db2Region, tableName) + "t1.RS_CREATE_DTM = t2.RS_CREATE_DTM AND "
				+tempWhereClause(primaryColumnList) + ") ";
		String mainSelectStatement = " SELECT "+SINGLEQUOTE+trimTableName(tableName) + SINGLEQUOTE+" AS TABLE_NAME, "+SINGLEQUOTE+"GREENPLUM"+SINGLEQUOTE
				+" AS DB_SIDE "+ tempSumColumns(sumColumns) + ", SUM(COUNT_"+trimTableName(tableName)+") AS COUNT_"+trimTableName(tableName) + " FROM (";
		
		String firstInnerSelect = " SELECT "+createFirstInnerSelect(sumColumns) + COUNT + " AS COUNT_"+trimTableName(tableName)+ " FROM chs"
				+db2Region.getRegion()+DOT+trimTableName(tableName)+ " TA ";
		
		String secondInnerSelect = " SELECT "+createSecondInnerSelect(sumColumns) + COUNT + " * -1 AS COUNT_"+trimTableName(tableName)+ " FROM chs"
				+db2Region.getRegion()+DOT+trimTableName(tableName)+ " INNERTA ";
		
		String innerJoin = " INNER JOIN LATEST_RS_"+trimTableName(tableName) + " INNERRS ON " + tempWhereClause(primaryColumnList).replaceAll("t1", "INNERTA");
		innerJoin = innerJoin.replaceAll("t2", "INNERRS");
		
		String thirdInnerSelect = " SELECT "+getThirdInnerSelect(sumColumns, tableName) +" FROM LATEST_RS_"+trimTableName(tableName) + " WHERE ACTION_TYP IN (0, 1)";
		
		combinedTASumQuery += tempQuery + mainSelectStatement + firstInnerSelect + " UNION ALL "+ secondInnerSelect + innerJoin + " UNION ALL" + 
				thirdInnerSelect + " ) AS FULL_"+trimTableName(tableName);
		

		return combinedTASumQuery.toLowerCase().replace("truncate", "trunc");
	}
	
	

	/**
	 * Query for Greenplum and Total Acces comparison to run in greenplum
	 *
	 * @param region - the region
	 * @param tablename - the tablename
	 * @param sumColumnList - the sum column list
	 * @return The query to run in greenplum for total access and greenplum comparison
	 */
	public static String getGPQuery(DatabaseRegion region, String tablename, LinkedList<String> sumColumnList) {
		String newTrimmedTableName =trimTableName(tablename);
		
		if (updatedCountTableList != null || !updatedCountTableList.isEmpty()) {
			if (updatedCountTableList.contains(newTrimmedTableName)) {
				String gpQuery = "SELECT VARCHAR "+SINGLEQUOTE + trimTableName(tablename) + SINGLEQUOTE + " AS TABLE_NAME, VARCHAR 'GREENPLUM' AS DB_SIDE, "
						+ COUNT + " AS COUNT_"+trimTableName(tablename) + " FROM chs"+region.getRegion() + DOT + trimTableName(tablename);   
				return gpQuery.toLowerCase().replace("truncate", "trunc");
			} else {
				String gpQuery = "SELECT VARCHAR "+SINGLEQUOTE + trimTableName(tablename) + SINGLEQUOTE + " AS TABLE_NAME, VARCHAR 'GREENPLUM' AS DB_SIDE, "
						+getCoreSideSumColumnString(sumColumnList) + COUNT + " AS COUNT_"+trimTableName(tablename) + " FROM chs"+region.getRegion() + DOT + trimTableName(tablename);  
				return gpQuery.toLowerCase().replace("truncate", "trunc");
			}
		} else {
			String gpQuery =  "SELECT VARCHAR "+SINGLEQUOTE + trimTableName(tablename) + SINGLEQUOTE + " AS TABLE_NAME, VARCHAR 'GREENPLUM' AS DB_SIDE, "
					+getCoreSideSumColumnString(sumColumnList) + COUNT + " AS COUNT_"+trimTableName(tablename) + " FROM chs"+region.getRegion() + DOT + trimTableName(tablename); 
			return gpQuery.toLowerCase().replace("truncate", "trunc");
		}
	}
	
	/**
	 * Query for Greenplum and Total Acces comparison to run in greenplum
	 *
	 * @param region - the region
	 * @param tablename - the tablename
	 * @param sumColumnList - the sum column list
	 * @return The query to run in greenplum for total access and greenplum comparison
	 */
	public static String getGPArchiveQuery(DatabaseRegion region, String tablename, LinkedList<String> sumColumnList) {
		String newTrimmedTableName = tablename;
		
		if (updatedCountTableList != null || !updatedCountTableList.isEmpty()) {
			if (updatedCountTableList.contains(newTrimmedTableName)) {
				String gpQuery = "SELECT VARCHAR "+SINGLEQUOTE + tablename + SINGLEQUOTE + " AS TABLE_NAME, VARCHAR 'GREENPLUM' AS DB_SIDE, "
						+ COUNT + " AS COUNT_"+tablename + " FROM "+ARCHIVESHCEMAPREFIX+region.getRegion() + DOT + tablename;   
				return gpQuery.toLowerCase().replace("truncate", "trunc");
			} else {
				String gpQuery = "SELECT VARCHAR "+SINGLEQUOTE + tablename + SINGLEQUOTE + " AS TABLE_NAME, VARCHAR 'GREENPLUM' AS DB_SIDE, "
						+getCoreSideSumColumnString(sumColumnList) + COUNT + " AS COUNT_"+tablename + " FROM "+ARCHIVESHCEMAPREFIX+region.getRegion() + DOT + tablename;  
				return gpQuery.toLowerCase().replace("truncate", "trunc");
			}
		} else {
			String gpQuery =  "SELECT VARCHAR "+SINGLEQUOTE + tablename + SINGLEQUOTE + " AS TABLE_NAME, VARCHAR 'GREENPLUM' AS DB_SIDE, "
					+getCoreSideSumColumnString(sumColumnList) + COUNT + " AS COUNT_"+tablename + " FROM "+ARCHIVESHCEMAPREFIX+region.getRegion() + DOT + tablename; 
			return gpQuery.toLowerCase().replace("truncate", "trunc");
		}
	}
	
	/**
	 * Gets the TA count query.
	 *
	 * @param db2Region - the region
	 * @param tableName - the table name
	 * @param primaryColumnList - the primary column list
	 * @param object - Object of commit_dtm to get all records those are after inserted that time in RS_* table
	 * @return the TA count query
	 */
	public static String getTACountQuery(DatabaseRegion db2Region, String tableName, LinkedList<String> primaryColumnList, Object object) {
		
		Set<String> linkedHashSet = new LinkedHashSet<String>();
		
		logger.debug("Table name with schema name: "+tableName);
		logger.debug("Table Name without schema: "+trimTableName(tableName));
		for (String string : primaryColumnList) {
			linkedHashSet.add(string);
		}
		
		LinkedList<String> taTablesList = new LinkedList<String>();
		for (int i = 0; i < TATABLES.length; i++) {
			taTablesList.add(TATABLES[i]);
		}
		
		//Checks for the schema prefix. Some tables use TA schemas
		if (taTablesList.contains(trimTableName(tableName))) {
			taChs = "TA";
			logger.debug(trimTableName(tableName) + " uses TA schema.");
		} else {
			taChs = SCHEMAPREFIX;
			logger.debug(trimTableName(tableName) + " uses CHS schema.");
		} 
		
		String combinedTACountQuery = "";
		
		String tempQuery = getTaWithString(tableName, linkedHashSet) + " AS ( SELECT " + selectT1(linkedHashSet) + " FROM "+SCHEMAPREFIX+db2Region.getRegion()+DOT+"RS_"
				+trimTableName(tableName) + " t1, (" + innerSelect(primaryColumnList, object, db2Region, tableName) + "t1.RS_CREATE_DTM = t2.RS_CREATE_DTM AND "
				+tempWhereClause(primaryColumnList) + ") ";
		String mainSelectStatement = " SELECT "+SINGLEQUOTE+trimTableName(tableName) + SINGLEQUOTE+" AS TABLE_NAME, "+SINGLEQUOTE+"TOTAL_ACCESS"+SINGLEQUOTE
				+" AS DB_SIDE, SUM(COUNT_"+trimTableName(tableName)+") AS COUNT_"+trimTableName(tableName) + " FROM (";
		
		String firstInnerSelect = " SELECT " + COUNT + " AS COUNT_"+trimTableName(tableName)+ " FROM "+taChs
				+db2Region.getRegion()+DOT+trimTableName(tableName)+ " TA ";
		
		String secondInnerSelect = " SELECT "+ COUNT + " * -1 AS COUNT_"+trimTableName(tableName)+ " FROM "+ taChs
				+db2Region.getRegion()+DOT+trimTableName(tableName)+ " INNERTA ";
		
		String innerJoin = " INNER JOIN LATEST_RS_"+trimTableName(tableName) + " INNERRS ON " + tempWhereClause(primaryColumnList).replaceAll("t1", "INNERTA");
		innerJoin = innerJoin.replaceAll("t2", "INNERRS");
		
		String thirdInnerSelect = " SELECT "+COUNT+" AS COUNT_"+trimTableName(tableName) +" FROM LATEST_RS_"+trimTableName(tableName) + " WHERE ACTION_TYP IN (0, 1)";
		
		combinedTACountQuery += tempQuery + mainSelectStatement + firstInnerSelect + " UNION ALL "+ secondInnerSelect + innerJoin + " UNION ALL" + 
				thirdInnerSelect + " ) AS FULL_"+trimTableName(tableName);
		
		return combinedTACountQuery;
	}
	
	/**
	 * Gets the Greenplum count query.
	 *
	 * @param db2Region - the region
	 * @param tableName - the table name
	 * @param primaryColumnList - the primary column list
	 * @param object - Object of commit_dtm to get all records those are after inserted that time in RS_* table
	 * @return the TA count query
	 */
	public static String getCOREGPCountQuery(DatabaseRegion db2Region, String tableName, LinkedList<String> primaryColumnList, Object object) {
		
		Set<String> linkedHashSet = new LinkedHashSet<String>();
		
		for (String string : primaryColumnList) {
			linkedHashSet.add(string);
		}
		
		String combinedTACountQuery = "";
		
		String tempQuery = getTaWithString(tableName, linkedHashSet) + " AS ( SELECT " + selectT1(linkedHashSet) + " FROM "+SCHEMAPREFIX+db2Region.getRegion()+DOT+"RS_"
				+trimTableName(tableName) + " t1, (" + innerSelect(primaryColumnList, object, db2Region, tableName) + "t1.RS_CREATE_DTM = t2.RS_CREATE_DTM AND "
				+tempWhereClause(primaryColumnList) + ") ";
		String mainSelectStatement = " SELECT "+SINGLEQUOTE+trimTableName(tableName) + SINGLEQUOTE+" AS TABLE_NAME, "+SINGLEQUOTE+"GREENPLUM"+SINGLEQUOTE
				+" AS DB_SIDE, SUM(COUNT_"+trimTableName(tableName)+") AS COUNT_"+trimTableName(tableName) + " FROM (";
		
		String firstInnerSelect = " SELECT " + COUNT + " AS COUNT_"+trimTableName(tableName)+ " FROM chs"+db2Region.getRegion()+DOT+trimTableName(tableName)+ " TA ";
		
		String secondInnerSelect = " SELECT "+ COUNT + " * -1 AS COUNT_"+trimTableName(tableName)+ " FROM chs"+db2Region.getRegion()+DOT+trimTableName(tableName)+ " INNERTA ";
		
		String innerJoin = " INNER JOIN LATEST_RS_"+trimTableName(tableName) + " INNERRS ON " + tempWhereClause(primaryColumnList).replaceAll("t1", "INNERTA");
		innerJoin = innerJoin.replaceAll("t2", "INNERRS");
		
		String thirdInnerSelect = " SELECT "+COUNT+" AS COUNT_"+trimTableName(tableName) +" FROM LATEST_RS_"+trimTableName(tableName) + " WHERE ACTION_TYP IN (0, 1)";
		
		combinedTACountQuery += tempQuery + mainSelectStatement + firstInnerSelect + " UNION ALL "+ secondInnerSelect + innerJoin + " UNION ALL" + 
				thirdInnerSelect + " ) AS FULL_"+trimTableName(tableName);
		
		return combinedTACountQuery.toLowerCase();
	}

	private static String getThirdInnerSelect(LinkedList<String> sumColumns, String tableName) {
		String thirdInnerSelect = "";
		for (String string : sumColumns) {
			thirdInnerSelect += SUM.replace("_", "(")+CAST+"(TRUNCATE(LATEST_RS_"+trimTableName(tableName)+DOT+string+"))"+ASDECIMAL+")) AS "+SUM+string+ ", ";
		}
			
			thirdInnerSelect += COUNT+" AS COUNT_"+trimTableName(tableName);
		
		return thirdInnerSelect;
	}


	private static String createSecondInnerSelect(LinkedList<String> sumColumns) {
		String innerSelect2Columns = "";
		for (String string : sumColumns) {
			innerSelect2Columns += SUM.replace("_", "(")+CAST+"(TRUNCATE(INNERTA"+DOT+string+"))"+ASDECIMAL+")) * -1 AS "+SUM+string + ", ";
		}
		return innerSelect2Columns;
	}


	private static String createFirstInnerSelect(LinkedList<String> sumColumns) {
		String innerSelect1Columns = "";
		for (String string : sumColumns) {
			innerSelect1Columns += SUM.replace("_", "(")+CAST+"(TRUNCATE(TA"+DOT+string+"))"+ASDECIMAL+")) AS "+SUM+string + ", ";
		}
		
		return innerSelect1Columns;
	}


	private static String tempSumColumns(LinkedList<String> sumColumns) {
		String tempSumColumn = "";
		for (String string : sumColumns) {
			tempSumColumn += ", SUM(SUM_"+string+") AS SUM_"+string;
		}
		return tempSumColumn;
	}


	private static String innerSelect(LinkedList<String> primaryColumnList, Object object, DatabaseRegion db2Region, String tablename) {
		String innerSelect =  "SELECT "+primaryKeyString(primaryColumnList) + ", MAX (RS_CREATE_DTM) AS RS_CREATE_DTM FROM "+SCHEMAPREFIX+db2Region.getRegion()+DOT
					+RS+trimTableName(tablename) + " WHERE COMMIT_DTM <= "+SINGLEQUOTE+object+SINGLEQUOTE+" GROUP BY "+primaryKeyString(primaryColumnList) + ") t2 WHERE ";
		
		return innerSelect;
	}


	private static String tempWhereClause(LinkedList<String> primaryColumnList) {
		
		String tempWhereClause = "";
		for (String string : primaryColumnList) {
			tempWhereClause += " t1"+DOT+string + " = t2"+DOT+string + " AND ";
		}
		
		tempWhereClause = tempWhereClause.substring(0, tempWhereClause.length() - 5);
		return tempWhereClause;
	}


	private static String primaryKeyString(LinkedList<String> primaryColumnList) {
		String keyString = "";
		for (String string : primaryColumnList) {
			keyString += string + ", ";
		}
		
		keyString = keyString.substring(0, keyString.length() - 2);
		
		return keyString;
	}


	private static String selectT1(Set<String> linkedHashSet) {
		
		String columnString = "";
		for (String string : linkedHashSet) {
			columnString += "t1"+DOT+string +", ";
		}
		
		columnString += "t1.ACTION_TYP ";
		
		return columnString;
	}


	private static String getTaWithString(String tableName, Set<String> linkedHashSet) {
		String with = "WITH LATEST_RS_"+trimTableName(tableName)+" (";
		
		String columnString = "";
		for (String string : linkedHashSet) {
			columnString += string +", ";
		}
		
		columnString += " ACTION_TYP";
		with = with + columnString + " )";
		
		return with;
	}


	public static String trimTableName(String tableName) {
		
		Integer index = tableName.lastIndexOf(".");
		String trimmedTableName = null;
		if (tableName.startsWith("OEDCOD.TED")) {
			trimmedTableName = tableName.substring(index + 2, tableName.length());
		} else if (tableName.startsWith(SCHEMAPREFIX+databaseRegion.getRegion()+DOT+"TED")) {
			trimmedTableName = tableName.substring(index + 2, tableName.length());
		} else {
			trimmedTableName = tableName.substring(index + 1, tableName.length());
		}
		
		trimmedTableName = trimmedTableName.replace("ED_SCHL_EXPRO_HST", "ED_SCHL_EXPRO_HIST");
		return trimmedTableName;
	}


	private static String getTableNameStringForSumColumns(LinkedList<String> tableList) {
		
		String tableNames = "";
		for (String string : tableList) {
			String modifiedEdTable;
			if (string.startsWith("ED_")) {
				modifiedEdTable = string.replace("ED_", "TED_");
				tableNames += SINGLEQUOTE + modifiedEdTable + SINGLEQUOTE +", ";
				logger.debug("All TED tables name has been changed to ED for Total Access.");;
			} else {
				tableNames += SINGLEQUOTE + string + SINGLEQUOTE +", ";
			}
		}
		
		tableNames = tableNames.substring(0, tableNames.length() - 2);
		
		return tableNames;
	}
	
	private static String getTableNamesStringForPrimaryKey(LinkedHashSet<String> tableList) {
		Integer i = 0;
		String tableNames = "";
		
		for (String string : tableList) {
			tableNames += SINGLEQUOTE + RS.toLowerCase() + trimTableName(string).toLowerCase() + SINGLEQUOTE +", ";
			i++;
		}
	
		tableNames = tableNames.substring(0, tableNames.length() - 2);
		tableNames = tableNames.replace("rs_ed_schl_expro_hst", "rs_ed_schl_expro_hist");
		
		logger.debug("The number of tables in the query are: "+i);
		return tableNames;
	}
	
	private static String getCoreSideSumColumnString(LinkedList<String> columnsList) {
		String columns = "";
		for (String string : columnsList) {
			columns += " SUM(CAST((TRUNCATE("+string.toUpperCase()+")) "+ASDECIMAL+ ")) AS SUM_"+string + ", ";
		}
		
		return columns;
	}
	
	private static String getColumnsForCount(LinkedList<String> noSumTables) {
		String columnsString = "";
		for (String string : noSumTables) {
			columnsString += SINGLEQUOTE+string+SINGLEQUOTE+", ";
		}
		
		columnsString = columnsString.substring(0, columnsString.length() - 2);
		
		return columnsString;
	}

	public static LinkedList<String> getCountTablesList() {
		return countTablesList;
	}

	public static void setCountTablesList(LinkedList<String> countTablesList) {
		CheckSumSQLGenerator.countTablesList = countTablesList;
	}

}
