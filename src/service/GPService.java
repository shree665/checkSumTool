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
import dao.GPDao;
/**
 * Service layer to map the resultset into the objects
 * 
 * @author vivek.subedi
 *
 */
public class GPService {
	private static Logger logger = Logger.getLogger(GPService.class);
	
	private GPDao gpDao;
	private LinkedList<String> tableNameList;
	
	public GPService(String db2UserName, String db2Password, String gpUserName, String gpPassword, DatabaseRegion region) {
		gpDao = new GPDao(db2UserName, db2Password, gpUserName, gpPassword, region);
	}
	
	/**
	 * Rep table names.
	 *
	 * @param region - the DB2 region
	 */
	public void coreRepTableNames(DatabaseRegion region) throws SQLException{
		tableNameList = new LinkedList<String>();
		ResultSet resultSet = gpDao.getCoreRepTableNames(region);
		try {
			while(resultSet.next()) {
				tableNameList.add(resultSet.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Ta rep table names.
	 *
	 * @param region - DB2 region
	 * @throws SQLException the SQL exception
	 */
	public void taRepTableNames(DatabaseRegion region) throws SQLException{
		tableNameList = new LinkedList<String>();
		ResultSet resultSet = gpDao.getTaRepTableNames(region);
		try {
			while(resultSet.next()) {
				tableNameList.add(resultSet.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
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
		ResultSet resultSet = gpDao.getTaCountTableNames(region, tables);
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
	 */
	public Map<String, LinkedList<String>> getTASumColumns(DatabaseRegion region, LinkedList<String> selectedTableList){
		Map<String, LinkedList<String>> sumColumnsMap = new HashMap<String, LinkedList<String>>();
		LinkedList<String> sumColumnsofTable;
		ResultSet resultSet = gpDao.getTaSumColumns(region, selectedTableList);
		Integer a = 0;
		try {
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
			logger.debug("Something Wrong with SQL. Please check your SQL. The Error message is: "+e.getMessage());
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
					ResultSet resultSet = gpDao.getPrimaryKeyColumnsofTables(region, selectedTableList);
					while (resultSet.next()) {
						if (primaryKeyColumnMap.isEmpty()) {
							primaryColumnsOfTable = new LinkedList<String>();
							primaryColumnsOfTable.add(resultSet.getString("name").toUpperCase());
							primaryKeyColumnMap.put(resultSet.getString("tbname").toUpperCase(), primaryColumnsOfTable);
							a++;
						} else {
							if (primaryKeyColumnMap.containsKey(resultSet.getString("tbname").toUpperCase())) {
								primaryKeyColumnMap.get(resultSet.getString("tbname").toUpperCase()).add(resultSet.getString("name").toUpperCase());
							} else {
								primaryColumnsOfTable = new LinkedList<String>();
								primaryColumnsOfTable.add(resultSet.getString("name").toUpperCase());
								primaryKeyColumnMap.put(resultSet.getString("tbname").toUpperCase(), primaryColumnsOfTable);
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
	
	public LinkedList<String> getTableNameList() {
		return tableNameList;
	}


	/**
	 * Gets sum result set from the GREENPLUM to compare with the TOTAL ACCESS.
	 *
	 * @param region - Greenplum region
	 * @param tablename - the tablename
	 * @param sumColumnList - @LinkedList of sum columns of table
	 * @return Map of table and its sum objects
	 * @throws SQLException the SQL exception
	 */
	public Map<String, SumObject> getTAGPResultMap(DatabaseRegion region, String tablename, LinkedList<String> sumColumnList) throws SQLException {
		Map<String, SumObject> gpSumMap = new HashMap<String, SumObject>();
		String message = "";
		try {
			ResultSet resultSet = gpDao.getColumnSumOfGP(region, tablename, sumColumnList);
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while (resultSet.next()) {
				for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
					SumObject sumObject = new SumObject();
					String columnname;
					if (resultSetMetaData.getColumnName(i).trim().length() > 30) {
						columnname = resultSetMetaData.getColumnName(i).trim().substring(0, 30);
						logger.info("["+resultSetMetaData.getColumnName(i)+"] has been trimmed to ["+columnname+"]");
					} else {
						columnname = resultSetMetaData.getColumnName(i).trim();
					}
					sumObject.setColumnName(columnname.toUpperCase());
					sumObject.setColumnValue(resultSet.getObject(resultSetMetaData.getColumnName(i)));
					gpSumMap.put(columnname.toUpperCase(), sumObject);
					message += resultSetMetaData.getColumnName(i)+"="+resultSet.getObject(resultSetMetaData.getColumnName(i))+", ";
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}

		logger.debug("The resultset values are: "+message.substring(0, message.length()-2));
		return gpSumMap;
	}
	
	/**
	 * Gets sum result set from the GREENPLUM Archive to compare with the TOTAL ACCESS Archive.
	 *
	 * @param region - Greenplum region
	 * @param tablename - the tablename
	 * @param sumColumnList - @LinkedList of sum columns of table
	 * @return Map of table and its sum objects
	 * @throws SQLException the SQL exception
	 */
	public Map<String, SumObject> getGPArchive(DatabaseRegion region, String tablename, LinkedList<String> sumColumnList) throws SQLException {
		Map<String, SumObject> gpSumMap = new HashMap<String, SumObject>();
		String message = "";
		try {
			ResultSet resultSet = gpDao.getColumnSumOfGPArchive(region, tablename, sumColumnList);
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while (resultSet.next()) {
				for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
					SumObject sumObject = new SumObject();
					String columnname;
					if (resultSetMetaData.getColumnName(i).trim().length() > 30) {
						columnname = resultSetMetaData.getColumnName(i).trim().substring(0, 30);
						logger.info("["+resultSetMetaData.getColumnName(i)+"] has been trimmed to ["+columnname+"]");
					} else {
						columnname = resultSetMetaData.getColumnName(i).trim();
					}
					sumObject.setColumnName(columnname.toUpperCase());
					sumObject.setColumnValue(resultSet.getObject(resultSetMetaData.getColumnName(i)));
					gpSumMap.put(columnname.toUpperCase(), sumObject);
					message += resultSetMetaData.getColumnName(i)+"="+resultSet.getObject(resultSetMetaData.getColumnName(i))+", ";
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}

		logger.debug("The resultset values are: "+message.substring(0, message.length()-2));
		return gpSumMap;
	}

	
	/**
	 * Gets the sum of GREENPLUM database to compare with CORE.
	 *
	 * @param region - GP region
	 * @param tablename - the tablename
	 * @param primaryKeyColumnList - @LinkedList of primary key associated with the table
	 * @param sumColumns - @LinkedList of columns that needs to be summ associated with the table
	 * @param object - Object is commit dtm value to use in RS_* table
	 * @return @HashMap of table and its sum object
	 * @throws SQLException the SQL exception
	 */
	public Map<String, SumObject> getGPofCOREResultMap (DatabaseRegion region, String tablename, LinkedList<String> primaryKeyColumnList, LinkedList<String> sumColumns, Object object) throws SQLException{
		Map<String, SumObject> taSumMap = new HashMap<String, SumObject>();
		ResultSet resultSet = gpDao.getColumnSumofGPofCore(region, tablename, primaryKeyColumnList, sumColumns, object);
		String message = "";
		try {
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while (resultSet.next()) {
				for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
					SumObject sumObject = new SumObject();
					String columnname;
					if (resultSetMetaData.getColumnName(i).trim().length() > 30) {
						columnname = resultSetMetaData.getColumnName(i).trim().substring(0, 30);
						logger.info("["+resultSetMetaData.getColumnName(i)+"] has been trimmed to ["+columnname+"]");
					} else {
						columnname = resultSetMetaData.getColumnName(i).trim();
					}
					sumObject.setColumnName(columnname.toUpperCase());
					sumObject.setColumnValue(resultSet.getObject(resultSetMetaData.getColumnName(i)));
					taSumMap.put(columnname.toUpperCase(), sumObject);
					message += resultSetMetaData.getColumnName(i)+"="+resultSet.getObject(resultSetMetaData.getColumnName(i))+", ";
				}
			}
		} catch (SQLException e) {
			throw new SQLException(e);
		}
		logger.debug("The resultset values are: "+message.substring(0, message.length()-2));
		return taSumMap;
	}
}
