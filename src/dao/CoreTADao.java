/**
 * 
 */
package dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import persistence.DatabaseRegion;
import tools.CheckSumSQLGenerator;
import tools.DB2Connection;

/**
 * DAO Class to interact with CORE/TOTAL ACCESS of DB2 database.
 * 
 * @author vivek.subedi
 *
 */
public class CoreTADao {
	
	private static Logger logger = Logger.getLogger(CoreTADao.class);
	
	private DB2Connection connection;
	
	public CoreTADao(String username, String password, DatabaseRegion db2Region) {
		connection = new DB2Connection(db2Region.getCoreConnectionString(), db2Region.getTaConnectionString(), username, password);
	}

	
	/**
	 * Gets the rep table.
	 *
	 * @param region the region
	 * @return resultset @ResultSet of tables
	 * @throws SQLException 
	 */
	
	public ResultSet getRepTableNames(DatabaseRegion region) {
		ResultSet resultSet = null;
		
		String query = CheckSumSQLGenerator.getDB2RepTableQuery(region) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		resultSet = connection.executeCoreQuery(query);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - "  + query);
		
		return resultSet;
	}

	/**
	 * Gets Table names with its schema name from core those don't have sum columns.
	 *
	 * @param region - the db2 region
	 * @param tables -  list of tables that are displayed
	 * @return resultset of tables
	 * @throws SQLException 
	 */
	public ResultSet getCountTablesList(DatabaseRegion region, LinkedList<String> tables) {
		ResultSet resultSet = null;
		
		String query = CheckSumSQLGenerator.getTableNameWithSchemaforCountTable(region, tables);
		resultSet = connection.executeCoreQuery(query);
		logger.info(query);
		return resultSet;
	}
	
	/**
	 * Gets Table names with its schema name from core those don't have sum columns.
	 *
	 * @param region - the db2 region
	 * @param tables -  list of tables that are displayed
	 * @return resultset of tables
	 * @throws SQLException 
	 */
	public ResultSet getCountArchiveTablesList(DatabaseRegion region, LinkedList<String> tables) {
		ResultSet resultSet = null;
		
		String query = CheckSumSQLGenerator.getTableNameWithSchemaforCountTable(region, tables);
		resultSet = connection.executeCoreQuery(query);
		logger.info(query);
		return resultSet;
	}




	/**
	 * Gets the sum columns.
	 *
	 * @param region - the DB2 region
	 * @param selectedTableList - @LinkedList of the tables that are selected by user to process
	 * @return resultset - @ResultSet of all the columns that needs to be sum up for that tables that are selected by user
	 * @throws SQLException 
	 * 
	 */
	
	public ResultSet getSumColumns(DatabaseRegion region, LinkedList<String> selectedTableList) {
		ResultSet resultSet = null;
		
		String queryString = CheckSumSQLGenerator.getSumColumnQuery(region, selectedTableList) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		resultSet = connection.executeCoreQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		
		return resultSet;
	}


	/**
	 * Gets the primary key column of tables.
	 *
	 * @param region - DB2 Region
	 * @param selectedTableList - @LinkedList of the tables that are selected by user to process
	 * @return resultset - @ResultSet of all the primary keys of the selected tables
	 * @throws SQLException 
	 * 
	 */
	
	public ResultSet getPrimaryKeyColumnOfTables(DatabaseRegion region, LinkedHashSet<String> selectedTableList) {
		ResultSet resultSet = null;
		
		String queryString = CheckSumSQLGenerator.getPrimaryColumnQuery(region, selectedTableList) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		resultSet = connection.executeTaQuery(queryString.toUpperCase());
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString.toUpperCase());
		
		return resultSet;
	}
	
	/**
	 * Gets the resultset of core query.
	 *
	 * @param region - the DB2 region
	 * @param tableName - table name
	 * @param columnList - @LinkedList of columns that are being sum up for the table
	 * @param hourNeedsTobeAdded - the hours need to added to match GMT time for TA tables
	 * @return resultset - @ResultSet of the query
	 */
	
	public ResultSet getColumnSumOfCore(DatabaseRegion region, String tableName, LinkedList<String> columnList, Integer hourNeedsTobeAdded) {
		ResultSet resultSet = null;
		String queryString = CheckSumSQLGenerator.getCoreSumQuery(region, tableName, columnList, hourNeedsTobeAdded) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		logger.debug(queryString);
		resultSet = connection.executeCoreQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		
		return resultSet;
	}
	
	/**
	 * Gets the resultset of TA query.
	 *
	 * @param region - the DB2 region
	 * @param tableName - table name
	 * @param columnList - @LinkedList of columns that are being sum up for the table
	 * @param hourNeedsTobeAdded - the hours need to added to match GMT time for TA tables
	 * @return resultset - @ResultSet of the query
	 */
	
	public ResultSet getColumnSumOfTAforGP(DatabaseRegion region, String tableName, LinkedList<String> columnList, Integer hourNeedsTobeAdded) {
		ResultSet resultSet = null;
		String queryString = CheckSumSQLGenerator.getGPTASumQuery(region, tableName, columnList, hourNeedsTobeAdded) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		logger.debug(queryString);
		resultSet = connection.executeTaQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		
		return resultSet;
	}

	/**
	 * Gets the resultset of TA query.
	 *
	 * @param region - the DB2 region
	 * @param tableName - table name
	 * @param columnList - @LinkedList of columns that are being sum up for the table
	 * @param hourNeedsTobeAdded - the hours need to added to match GMT time for TA tables
	 * @return resultset - @ResultSet of the query
	 */
	
	public ResultSet getColumnSumOfTAforArchive(DatabaseRegion region, String tableName, LinkedList<String> columnList) {
		ResultSet resultSet = null;
		String queryString = CheckSumSQLGenerator.getArchiveTASumQuery(region, tableName, columnList) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		logger.debug(queryString);
		resultSet = connection.executeTaQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		
		return resultSet;
	}

	/**
	 * Gets the resultset of the TA query
	 *
	 * @param region - the DB2 region
	 * @param tablename - the tablename
	 * @param primaryKeyColumnList - @LinkedList of primary keys of a tables
	 * @param columnList - @LinkedList of columns that are being sum up for the table
	 * @param object - the @Timestamp of the core query that is executed in CORE. This timestamp already in GMT
	 * @return resultset - @ResultSet of the query
	 * @throws SQLException 
	 */
	
	public ResultSet getColumnSumOfTA(DatabaseRegion region, String tablename, LinkedList<String> primaryKeyColumnList, LinkedList<String> sumColumns, Object object) {
		ResultSet resultSet = null;
		
		if (sumColumns != null) {
			String queryString = CheckSumSQLGenerator.getTaSumQuery(region, tablename, primaryKeyColumnList, sumColumns, object) + CheckSumSQLGenerator.WITHUR;
			Long startTime = System.currentTimeMillis();
			logger.debug(queryString);
			resultSet = connection.executeTaQuery(queryString);
			Long endTime = System.currentTimeMillis();
			logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
			
			return resultSet;
		} else {
			String queryString = CheckSumSQLGenerator.getTACountQuery(region, tablename, primaryKeyColumnList, object) + CheckSumSQLGenerator.WITHUR;
			Long startTime = System.currentTimeMillis();
			logger.debug(queryString);
			resultSet = connection.executeTaQuery(queryString);
			Long endTime = System.currentTimeMillis();
			logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
			
			return resultSet;
		}
	}
	
	/**
	 * Gets the archive table.
	 *
	 * @param region the region
	 * @return resultset @ResultSet of tables
	 * @throws SQLException 
	 */
	
	public ResultSet getArchiveTableNames(DatabaseRegion region) {
		ResultSet resultSet = null;
		
		String query = CheckSumSQLGenerator.getDB2ArchiveTableQuery(region) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		resultSet = connection.executeTaQuery(query);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - "  + query);
		
		return resultSet;
	}
	
	/**
	 * Gets the sum columns.
	 *
	 * @param region - the DB2 region
	 * @param selectedTableList - @LinkedList of the tables that are selected by user to process
	 * @return resultset - @ResultSet of all the columns that needs to be sum up for that tables that are selected by user
	 * @throws SQLException 
	 * 
	 */
	
	public ResultSet getArchiveSumColumns(DatabaseRegion region, LinkedList<String> selectedTableList) {
		ResultSet resultSet = null;
		
		String queryString = CheckSumSQLGenerator.getArchiveSumColumnQuery(region, selectedTableList) + CheckSumSQLGenerator.WITHUR;
		Long startTime = System.currentTimeMillis();
		resultSet = connection.executeTaQuery(queryString);
		Long endTime = System.currentTimeMillis();
		logger.info(new Timestamp(new Date().getTime()) + " - ["+ (endTime - startTime)/1000+"s to execute this Query] - " + queryString);
		
		return resultSet;
	}
}
