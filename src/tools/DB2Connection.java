/**
 * 
 */
package tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import persistence.DatabaseRegion;

/**
 * @author vivek.subedi
 *
 */
public class DB2Connection {
	
	private static Logger logger = Logger.getLogger(DB2Connection.class);
	
	private String coreJdbcString;
	private String taJdbcString;
	private Connection coreConnection;
	private Connection taConnection;
	private String userName;
	private String passWord;

	/**
	 * Instantiates a new db2 connection.
	 *
	 * @param coreJdbcString - the core jdbcstring
	 * @param taJdbcString - the ta jdbcstring
	 * @param userName - the username
	 * @param passWord - the password
	 */
	public DB2Connection(String coreJdbcString, String taJdbcString , String userName, String passWord) {
		super();
		this.coreJdbcString = coreJdbcString;
		this.taJdbcString = taJdbcString;
		this.coreConnection = null;
		this.taConnection = null;
		this.userName = userName;
		this.passWord = passWord;
	}

	/**
	 * Connect to core database.
	 *
	 * @return true, if successful
	 */
	private boolean connectToCoreDatabase() {

		try {
			coreConnection = DriverManager.getConnection(coreJdbcString, userName, passWord);
		} catch (Exception e) {
			logger.info("Connection is not established with following credentials in CORE:\nConnection String: ["+coreJdbcString+"]");
			logger.debug(CommonCompareUtil.getStackTrace(e));
			closeCoreConnection();
			return false;
		}

		return true;
	}
	
	/**
	 * Connect to ta database.
	 *
	 * @return true, if successful
	 */
	private boolean connectToTADatabase() {

		try {
			taConnection = DriverManager.getConnection(taJdbcString, userName, passWord);
		} catch (Exception e) {
			logger.info("Connection is not established with following credentials in TA:\nConnection String: ["+taJdbcString+"]");
			logger.debug(CommonCompareUtil.getStackTrace(e));
			closeTAConnection();
			return false;
		}

		return true;
	}

	/**
	 * Close core connection.
	 */
	public void closeCoreConnection() {
		try {
			coreConnection.close();
		} catch (Exception e) {
			logger.info("Unable to close CORE database connection");
			logger.debug(CommonCompareUtil.getStackTrace(e));
		}
	}
	
	/**
	 * Close ta connection.
	 */
	public void closeTAConnection() {
		try {
			taConnection.close();
		} catch (Exception e) {
			logger.info("Unable to close TA database connection");
			logger.debug(CommonCompareUtil.getStackTrace(e));
		}
	}

	/**
	 * Checks if is core open.
	 *
	 * @return true, if is core open
	 */
	public boolean isCoreOpen() {

		if (coreConnection == null) {
			return false;
		}

		try {
			return (!coreConnection.isClosed());
		} catch (Exception e) {
			logger.debug(CommonCompareUtil.getStackTrace(e));
		}

		return false;
	}
	
	/**
	 * Checks if is TA open.
	 *
	 * @return true, if is TA open
	 */
	public boolean isTAOpen() {

		if (taConnection == null) {
			return false;
		}

		try {
			return (!taConnection.isClosed());
		} catch (Exception e) {
			logger.debug(CommonCompareUtil.getStackTrace(e));
		}

		return false;
	}

	/**
	 * Execute core query.
	 *
	 * @param query the query
	 * @return the result set
	 */
	public ResultSet executeCoreQuery(String query) {

		ResultSet rs = null;

		if (!isCoreOpen()) {
			boolean connected = connectToCoreDatabase();
			if (!connected) {
				logger.info("Trying to Connect again....");
				connectToCoreDatabase();
			}
		}
		try {
			Statement stmt = this.coreConnection.createStatement();
			rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			logger.info("Couldn't execute the query ["+query+"] in CORE with error\n["+e.getMessage()+"]");
			logger.debug(CommonCompareUtil.getStackTrace(e));
			return null;
		}

		return rs;
	}
	
	/**
	 * Execute ta query.
	 *
	 * @param query the query
	 * @return the result set
	 */
	public ResultSet executeTaQuery(String query) {

		ResultSet rs;

		if (!isTAOpen()) {
			boolean connected = connectToTADatabase();
			if (!connected) {
				logger.info("Trying to connect again...");
				connectToTADatabase();
			}
		}
		try {
			Statement stmt = this.taConnection.createStatement();
			rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			logger.info("Couldn't execute the query ["+query+"] in TA with following error\n["+e.getMessage()+"]");
			logger.debug(CommonCompareUtil.getStackTrace(e));
			return null;
		} 

		return rs;
	}

	/**
	 * Checks if is connectable.
	 *
	 * @return true, if is connectable
	 */
	public boolean isConnectable() {
		if (connectToTADatabase()) {
			closeTAConnection();
			return true;
		} else {
			closeCoreConnection();
		}

		return false;
	}

	/**
	 * Test connection.
	 *
	 * @param r - The region
	 * @param username - the username
	 * @param password - the password
	 * @return true, if successful
	 */
	@SuppressWarnings("finally")
	public static boolean testConnection(DatabaseRegion r, String username, String password) {
		Connection core = null;
		try {
			core = DriverManager.getConnection(r.getCoreConnectionString(), username, password);
			logger.info("Connection in DB2 tested Successfully....");
		} catch (Exception e) {
			logger.info("Connection is not established in DB2");
			logger.info(CommonCompareUtil.getStackTrace(e));
			try {
				core.close();
			} catch (SQLException e1) {
				logger.info(CommonCompareUtil.getStackTrace(e1));
			} finally {
				return false;
			}
		} 
		
		try {
			core.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
}
