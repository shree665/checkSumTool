/**
 * 
 */
package service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;

import persistence.DatabaseRegion;
import persistence.SumObject;
import dao.CoreTADao;

/**
 * @author vivek.subedi
 *
 */
public class CoreTAService {
	
	private static Logger logger = Logger.getLogger(CoreTAService.class);
	
	private static CoreTADao coreTADao;
	private LinkedList<String> tableNameList;
	
	public CoreTAService(String username, String password, DatabaseRegion db2Region) {
		coreTADao = new CoreTADao(username, password, db2Region);
	}
	
	/**
	 * Rep table names.
	 *
	 * @param region - the DB2 region
	 * @throws SQLException 
	 */
	public void repTableNames(DatabaseRegion region) throws SQLException {
		tableNameList = new LinkedList<String>();
		ResultSet resultSet = coreTADao.getRepTableNames(region);
		try {
			while(resultSet.next()) {
				tableNameList.add(resultSet.getString(1));
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}
	}
	
	/**
	 * Gets the list of tables with schema.
	 *
	 * @param region - db2 region
	 * @param tables - list of displayed tables
	 * @return @LinkedList of table names with schema
	 * @throws SQLException 
	 */
	public LinkedList<String> getCountTablesList(DatabaseRegion region, LinkedList<String> tables) throws SQLException{
		LinkedList<String> schemaTables = new LinkedList<String>();
		ResultSet resultSet = coreTADao.getCountTablesList(region, tables);
		try {
			while (resultSet.next()) {
				schemaTables.add(resultSet.getString(1));
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}
		
		return schemaTables;
	}
	
	/**
	 * Gets the list of tables with schema.
	 *
	 * @param region - db2 region
	 * @param tables - list of displayed tables
	 * @return @LinkedList of table names with schema
	 * @throws SQLException 
	 */
	public LinkedList<String> getCountArchiveTablesList(DatabaseRegion region, LinkedList<String> tables) throws SQLException{
		LinkedList<String> schemaTables = new LinkedList<String>();
		ResultSet resultSet = coreTADao.getCountTablesList(region, tables);
		try {
			while (resultSet.next()) {
				schemaTables.add(resultSet.getString(1));
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}
		
		return schemaTables;
	}
	
	/**
	 * Gets the map of columns that are being sum up of a table
	 *
	 * @param region - the DB2 region
	 * @param selectedTableList - @LinkedList of the tables that are selected by user to process
	 * @return @Map - the map of columns that are being sum up of a table
	 * @throws SQLException 
	 */
	public Map<String, LinkedList<String>> getSumColumns(DatabaseRegion region, LinkedList<String> selectedTableList) throws SQLException{
		Map<String, LinkedList<String>> sumColumnsMap = new HashMap<String, LinkedList<String>>();
		LinkedList<String> sumColumnsofTable;
		Integer a = 0;
		try {
		ResultSet resultSet = coreTADao.getSumColumns(region, selectedTableList);
			while (resultSet.next()) {
				if (sumColumnsMap.isEmpty()) {
					sumColumnsofTable = new LinkedList<String>();
					sumColumnsofTable.add(resultSet.getString("NAME"));
					sumColumnsMap.put(resultSet.getString("SCHEMA_TABLE"), sumColumnsofTable);
					a++;
				} else {
					if (sumColumnsMap.containsKey(resultSet.getString("SCHEMA_TABLE"))) {
						sumColumnsMap.get(resultSet.getString("SCHEMA_TABLE")).add(resultSet.getString("NAME"));
					} else {
						sumColumnsofTable = new LinkedList<String>();
						sumColumnsofTable.add(resultSet.getString("NAME"));
						sumColumnsMap.put(resultSet.getString("SCHEMA_TABLE"), sumColumnsofTable);
						a++;
					}
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}
		logger.debug("The number of tables are those have columns that needs to be summed is/are: "+a);
		return sumColumnsMap;
		
	}
	
	/**
	 * Gets the map of primary keys that are being used by a table.
	 *
	 * @param region the region
	 * @param selectedTableList - @LinkedList of the tables that are selected by user to process
	 * @return @Map - the map of columns that are primary key of a table
	 */
	public Map<String, LinkedList<String>> getPrimaryKeyTableMap(DatabaseRegion region, LinkedHashSet<String> selectedTableList) throws SQLException{
		Map<String, LinkedList<String>> primaryKeyColumnMap = new HashMap<String, LinkedList<String>>();
		LinkedList<String> primaryColumnsOfTable;
		if (!selectedTableList.isEmpty()) {
			Integer a = 0;
			try {
					ResultSet resultSet = coreTADao.getPrimaryKeyColumnOfTables(region, selectedTableList);
					while (resultSet.next()) {
						if (primaryKeyColumnMap.isEmpty()) {
							primaryColumnsOfTable = new LinkedList<String>();
							primaryColumnsOfTable.add(resultSet.getString("NAME"));
							primaryKeyColumnMap.put(resultSet.getString("TBNAME"), primaryColumnsOfTable);
							a++;
						} else {
							if (primaryKeyColumnMap.containsKey(resultSet.getString("TBNAME"))) {
								primaryKeyColumnMap.get(resultSet.getString("TBNAME")).add(resultSet.getString("NAME"));
							} else {
								primaryColumnsOfTable = new LinkedList<String>();
								primaryColumnsOfTable.add(resultSet.getString("NAME"));
								primaryKeyColumnMap.put(resultSet.getString("TBNAME"), primaryColumnsOfTable);
								a++;
							}
						}
					}
			} catch (SQLException e) {
				throw new SQLException(e);
			}
			logger.debug("The number of tables are that have primary key: "+a);
		}
		
		
		return primaryKeyColumnMap;
	}
	
	/**
	 * Gets the map of columns and its object.
	 *
	 * @param db2Region - the db2 region
	 * @param tableName - the table name
	 * @param columns  - @LinkedList of columns that are being sum up
	 * @param hourNeedsTobeAdded - hours that needed to add into the commit DTM to match the GMT
	 * @return @Map - map of sumobject and the column names of a table
	 * @throws SQLException 
	 */
	public  Map<String, SumObject> getCoreSumResultMap(DatabaseRegion db2Region, String tableName, LinkedList<String> columns, Integer hourNeedsTobeAdded) throws SQLException {

		Map<String, SumObject> coreSumMap = new HashMap<String, SumObject>();
		String message = "";
		try {
			ResultSet resultSet = coreTADao.getColumnSumOfCore(db2Region, tableName, columns, hourNeedsTobeAdded);
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while (resultSet.next()) {
				for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
					SumObject sumObject = new SumObject();
					sumObject.setColumnName(resultSetMetaData.getColumnName(i));
					sumObject.setColumnValue(resultSet.getObject(resultSetMetaData.getColumnName(i)));
					coreSumMap.put(resultSetMetaData.getColumnName(i), sumObject);
					message += resultSetMetaData.getColumnName(i)+"="+resultSet.getObject(resultSetMetaData.getColumnName(i))+", ";
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}

		logger.debug("The resultset values are: "+message.substring(0, message.length()-2));
		return coreSumMap;
	}

	/**
	 * Gets the TA column and its sumobject.
	 *
	 * @param region - the DB2 region
	 * @param tablename - the tablename
	 * @param primaryKeyColumnList - @LinkedList of primary keys of a table
	 * @param sumColumns - @LinkedList of columns that are being sum up
	 * @param object - the @Timestamp that the core query was exected in
	 * @return @Map - Map of column and sumbObject
	 * @throws SQLException 
	 */
	
	public Map<String, SumObject> getTASumResultMap(DatabaseRegion region, String tablename, LinkedList<String> primaryKeyColumnList, LinkedList<String> sumColumns, Object object) throws SQLException {
		
		Map<String, SumObject> taSumMap = new HashMap<String, SumObject>();
		ResultSet resultSet = coreTADao.getColumnSumOfTA(region, tablename, primaryKeyColumnList, sumColumns, object);
		String message = "";
		try {
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while (resultSet.next()) {
				for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
					SumObject sumObject = new SumObject();
					sumObject.setColumnName(resultSetMetaData.getColumnName(i));
					sumObject.setColumnValue(resultSet.getObject(resultSetMetaData.getColumnName(i)));
					taSumMap.put(resultSetMetaData.getColumnName(i), sumObject);
					message += resultSetMetaData.getColumnName(i)+"="+resultSet.getObject(resultSetMetaData.getColumnName(i))+", ";
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}
		logger.debug("The resultset values are: "+message.substring(0, message.length()-2));
		return taSumMap;
	}

	/**
	 * Gets the TOTAL ACCESS SUM for Greenplum Comparison.
	 *
	 * @param region - DB2 region
	 * @param tablename - the tablename
	 * @param columns - The list sum columns
	 * @param hourNeedsTobeAdded - the hour that needs to be added to match the GMT
	 * @return @HasMap of table name and its sum object
	 * @throws SQLException the SQL exception
	 */
	public Map<String, SumObject> getTASumforGP(DatabaseRegion region,String tablename, LinkedList<String> columns,Integer hourNeedsTobeAdded) throws SQLException {

		Map<String, SumObject> coreSumMap = new HashMap<String, SumObject>();
		String message = "";
		try {
			ResultSet resultSet = coreTADao.getColumnSumOfTAforGP(region, tablename, columns, hourNeedsTobeAdded);
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while (resultSet.next()) {
				for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
					SumObject sumObject = new SumObject();
					sumObject.setColumnName(resultSetMetaData.getColumnName(i));
					sumObject.setColumnValue(resultSet.getObject(resultSetMetaData.getColumnName(i)));
					coreSumMap.put(resultSetMetaData.getColumnName(i), sumObject);
					message += resultSetMetaData.getColumnName(i)+"="+resultSet.getObject(resultSetMetaData.getColumnName(i))+", ";
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}

		logger.debug("The resultset values are: "+message.substring(0, message.length()-2));
		return coreSumMap;
	}
	
	/**
	 * Gets the TOTAL ACCESS SUM for Greenplum Comparison.
	 *
	 * @param region - DB2 region
	 * @param tablename - the tablename
	 * @param columns - The list sum columns
	 * @param hourNeedsTobeAdded - the hour that needs to be added to match the GMT
	 * @return @HasMap of table name and its sum object
	 * @throws SQLException the SQL exception
	 */
	public Map<String, SumObject> getTASumforArchive(DatabaseRegion region, String tablename, LinkedList<String> columns) throws SQLException {

		Map<String, SumObject> coreSumMap = new HashMap<String, SumObject>();
		String message = "";
		try {
			ResultSet resultSet = coreTADao.getColumnSumOfTAforArchive(region, tablename, columns);
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while (resultSet.next()) {
				for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
					SumObject sumObject = new SumObject();
					sumObject.setColumnName(resultSetMetaData.getColumnName(i));
					sumObject.setColumnValue(resultSet.getObject(resultSetMetaData.getColumnName(i)));
					coreSumMap.put(resultSetMetaData.getColumnName(i), sumObject);
					message += resultSetMetaData.getColumnName(i)+"="+resultSet.getObject(resultSetMetaData.getColumnName(i))+", ";
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}

		logger.debug("The resultset values are: "+message.substring(0, message.length()-2));
		return coreSumMap;
	}
	
	/**
	 * Archive table names.
	 *
	 * @param region - the DB2 region
	 * @throws SQLException 
	 */
	public void archiveTableNames(DatabaseRegion region) throws SQLException {
		tableNameList = new LinkedList<String>();
		ResultSet resultSet = coreTADao.getArchiveTableNames(region);
		try {
			while(resultSet.next()) {
				tableNameList.add(resultSet.getString(1));
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}
	}
	
	/**
	 * Gets the map of columns that are being sum up of a table
	 *
	 * @param region - the DB2 region
	 * @param selectedTableList - @LinkedList of the tables that are selected by user to process
	 * @return @Map - the map of columns that are being sum up of a table
	 * @throws SQLException 
	 */
	public Map<String, LinkedList<String>> getArchiveSumColumns(DatabaseRegion region, LinkedList<String> selectedTableList) throws SQLException{
		Map<String, LinkedList<String>> sumColumnsMap = new HashMap<String, LinkedList<String>>();
		LinkedList<String> sumColumnsofTable;
		Integer a = 0;
		try {
		ResultSet resultSet = coreTADao.getArchiveSumColumns(region, selectedTableList);
			while (resultSet.next()) {
				if (sumColumnsMap.isEmpty()) {
					sumColumnsofTable = new LinkedList<String>();
					sumColumnsofTable.add(resultSet.getString("NAME"));
					sumColumnsMap.put(resultSet.getString("SCHEMA_TABLE"), sumColumnsofTable);
					a++;
				} else {
					if (sumColumnsMap.containsKey(resultSet.getString("SCHEMA_TABLE"))) {
						sumColumnsMap.get(resultSet.getString("SCHEMA_TABLE")).add(resultSet.getString("NAME"));
					} else {
						sumColumnsofTable = new LinkedList<String>();
						sumColumnsofTable.add(resultSet.getString("NAME"));
						sumColumnsMap.put(resultSet.getString("SCHEMA_TABLE"), sumColumnsofTable);
						a++;
					}
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}
		logger.debug("The number of tables are those have columns that needs to be summed is/are: "+a);
		return sumColumnsMap;
		
	}
	
	public LinkedList<String> getTableNameList() {
		return tableNameList;
	}
}
