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
public class GPConnection {
	
	private static Logger logger = Logger.getLogger(GPConnection.class);

		private String jdbcString;
		private Connection connection;
		private String userName;
		private String passWord;

		/**
		 * Instantiates a new GP connection.
		 *
		 * @param jdbcString - the jdbcstring
		 * @param userName - the username
		 * @param passWord - the password
		 */
		public GPConnection(String jdbcString, String userName, String passWord) {
			super();
			this.jdbcString = jdbcString;
			this.connection = null;
			this.userName = userName;
			this.passWord = passWord;
		}

		/**
		 * Connect to Greenplum database.
		 *
		 * @return true, if successful
		 */
		private boolean connectToDatabase() {

			try {
				connection = DriverManager.getConnection(jdbcString,
						userName, passWord);
			} catch (Exception e) {
				closeConnection();
				return false;
			}

			return true;
		}

		/**
		 * Close connection.
		 */
		public void closeConnection() {
			try {
				connection.close();
			} catch (Exception e) {
				// could already be closed
				logger.info("Unable to close database connection");
			
			}
		}

		/**
		 * Checks if is open.
		 *
		 * @return true, if is open
		 */
		public boolean isOpen() {

			if (connection == null) {
				return false;
			}

			try {
				return (!connection.isClosed());
			} catch (Exception e) {
				logger.debug(CommonCompareUtil.getStackTrace(e));
			}

			return false;
		}

		/**
		 * Execute query in greenplum.
		 *
		 * @param query the query
		 * @return the result set
		 */
		public ResultSet executeQuery(String query) {

			ResultSet rs;

			if (!isOpen()) {
				boolean connected = connectToDatabase();
				if (!connected) {
					return null;
				}
			}
			try {
				Statement stmt = this.connection.createStatement();
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
			if (connectToDatabase()) {
				closeConnection();
				return true;
			} else {
				closeConnection();
			}

			return false;
		}

		/**
		 * Test connection.
		 *
		 * @param r - the greenplum region
		 * @param username the username
		 * @param password the password
		 * @return true, if successful
		 */
		@SuppressWarnings("finally")
		public static boolean testConnection(DatabaseRegion r, String username, String password) {
			Connection c = null;
			try {
				c = DriverManager.getConnection(r.getGreenplumConnectionString(), username, password);
				logger.info("Connection in Greenplum tested successfully....");
			} catch (Exception e) {
				logger.info("Connection is not established in Greenplum.");
				logger.info(CommonCompareUtil.getStackTrace(e));
				try {
					c.close();
				} catch (SQLException e1) {
					logger.info(CommonCompareUtil.getStackTrace(e1));
				} finally {
					return false;
				}
			} 
			
			try {
				c.close();
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}

}
