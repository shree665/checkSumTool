/**
 * 
 */
package dao;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import persistence.DatabaseRegion;
import tools.CheckSumSQLGenerator;
import tools.DB2Connection;
import tools.GPConnection;

/**
 * DAO class to interact with Greenplum Database and DB2 Databse depending on the user's choice.
 * 
 * @author vivek.subedi
 *
 */
public class GPDao {

	private static Logger logger = Logger.getLogger(GPDao.class);
	
	private DB2Connection db2Connection;
	private GPConnection gpConnection;
	
	public GPDao(String db2Username, String db2Password, String gpUsername, String gpPassword, DatabaseRegion db2Region) {
		db2Connection = new DB2Connection(db2Region.getCoreConnectionString(), db2Region.getTaConnectionString(), db2Username, db2Password);
		gpConnection = new GPConnection(db2Region.getGreenplumConnectionString(), gpUsername, gpPassword);
	}


	/**
	 * Gets all replicated tables for Greenplum using the DB2 CORE publication.
	 *
	 * @param region - DB2 Region
	 * @return the resultset of all the rep tables
	 */
	public ResultSet getCoreRepTableNames(DatabaseRegion region) {
		ResultSet resultSet = null;
		
		String query = CheckSumSQLGenerator.getGreenplumCoreRepTableQuery(region) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		resultSet = db2Connection.executeCoreQuery(query);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - "  + query);
		return resultSet;
	}
	
	/**
	 * Gets all replicated tables for Greenplum using the DB2 TOTAL ACCESS publication.
	 *
	 * @param region the region
	 * @return the ta rep table names
	 */
	public ResultSet getTaRepTableNames(DatabaseRegion region) {
		ResultSet resultSet = null;
		String query = CheckSumSQLGenerator.getGreenplumTARepTableQuery(region) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		resultSet = db2Connection.executeTaQuery(query);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - "  + query);
		return resultSet;
	}
	
	/**
	 * Gets all Count tables for Greenplum using the DB2 TOTAL ACCESS
	 *
	 * @param region the region
	 * @return the ta tables
	 */
	public ResultSet getTaCountTableNames(DatabaseRegion region, LinkedList<String> tables) {
		ResultSet resultSet = null;
		String query = CheckSumSQLGenerator.getTableNameWithSchemaforCountTable(region, tables) + CheckSumSQLGenerator.WITHUR;
		resultSet = db2Connection.executeTaQuery(query);
		return resultSet;
	}
	
	/**
	 * Gets the TOTAL ACCESS sum columns for the TOTAL ACCESS to Greenplum comparison.
	 *
	 * @param region the region
	 * @param selectedTableList the selected table list
	 * @return the ta sum columns
	 */
	public ResultSet getTaSumColumns(DatabaseRegion region, LinkedList<String> selectedTableList) {
		ResultSet resultSet = null;
		String queryString = CheckSumSQLGenerator.getTASumColumnQuery(region, selectedTableList) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		resultSet = db2Connection.executeTaQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		return resultSet;
	}
	
	/**
	 * Gets the primary key columns of tables.
	 *
	 * @param region - Greenplum region
	 * @param selectedTableList - @LinkedHashSet the user selected list of tables
	 * @return resultSet of the primary keys 
	 */
	public ResultSet getPrimaryKeyColumnsofTables(DatabaseRegion region, LinkedHashSet<String> selectedTableList) {
		ResultSet resultSet = null;
		String queryString = CheckSumSQLGenerator.getGPPrimaryColumnQuery(region, selectedTableList)+";";
		Long startTime = System.currentTimeMillis();
		logger.debug(queryString);
		resultSet = gpConnection.executeQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		return resultSet;
	}
	
	/**
	 * Gets the column sum of TOTAL ACCESS.
	 *
	 * @param region - DB2 region
	 * @param tableName - table name
	 * @param columnList - List of columns that needs to be sum
	 * @param hourNeedsTobeAdded - the hours needed to be added because of the day light saving
	 * @return result set of the TOTAL ACCESS query that retrives the summation
	 */
	public ResultSet getColumnSumofTA(DatabaseRegion region, String tableName, LinkedList<String> columnList, Integer hourNeedsTobeAdded) {
		ResultSet resultSet = null;
		String queryString = CheckSumSQLGenerator.getGPTASumQuery(region, tableName, columnList, hourNeedsTobeAdded) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		logger.debug(queryString);
		resultSet = db2Connection.executeTaQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		
		return resultSet;
	}

	/**
	 * Gets the column sum of GREENPLUM database to compare with the TOTAL ACCESS column sum.
	 *
	 * @param region - Greenplum region
	 * @param tablename - the tablename
	 * @param sumColumnList - list of the columns that needs to sum for the specific column in Greenplum
	 * @return Resultset of the columns
	 */
	public ResultSet getColumnSumOfGP(DatabaseRegion region, String tablename, LinkedList<String> sumColumnList) {
		ResultSet resultSet = null;
		String queryString = CheckSumSQLGenerator.getGPQuery(region, tablename, sumColumnList);
		Long startTime = System.currentTimeMillis();
		logger.debug(queryString);
		resultSet = gpConnection.executeQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		
		return resultSet;
	}
	
	/**
	 * Gets the column sum of GREENPLUM database to compare with the TOTAL ACCESS column sum.
	 *
	 * @param region - Greenplum region
	 * @param tablename - the tablename
	 * @param sumColumnList - list of the columns that needs to sum for the specific column in Greenplum
	 * @return Resultset of the columns
	 */
	public ResultSet getColumnSumOfGPArchive(DatabaseRegion region, String tablename, LinkedList<String> sumColumnList) {
		ResultSet resultSet = null;
		String queryString = CheckSumSQLGenerator.getGPArchiveQuery(region, tablename, sumColumnList);
		Long startTime = System.currentTimeMillis();
		logger.debug(queryString);
		resultSet = gpConnection.executeQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		
		return resultSet;
	}
	
	/**
	 * Gets the column sum of GREENPLUM database to compare with the CORE column's sum.
	 *
	 * @param region - Greenplum region
	 * @param tablename - the tablename
	 * @param primaryKeyColumnList - @LinkedList of primary key
	 * @param sumColumns @LinkedList of columns that needs to be sum
	 * @param object - commit_dtm object which we will be using to grab all the records from RS_* tables
	 * @return The resultset of the columns
	 */
	public ResultSet getColumnSumofGPofCore(DatabaseRegion region, String tablename, LinkedList<String> primaryKeyColumnList, LinkedList<String> sumColumns, Object object) {
		
		ResultSet resultSet = null;
		if (sumColumns != null) {
			String queryString = CheckSumSQLGenerator.getCOREGPQuery(region, tablename, primaryKeyColumnList, sumColumns, object);
			Long startTime = System.currentTimeMillis();
			logger.debug(queryString);
			resultSet = gpConnection.executeQuery(queryString);
			Long endTime = System.currentTimeMillis();
			logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		} else {
			String queryString = CheckSumSQLGenerator.getCOREGPCountQuery(region, tablename, primaryKeyColumnList, object);
			Long startTime = System.currentTimeMillis();
			logger.debug(queryString);
			resultSet = gpConnection.executeQuery(queryString);
			Long endTime = System.currentTimeMillis();
			logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		}
		
		return resultSet;
	}
}
