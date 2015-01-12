/**
 * 
 */
package gui;

import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.LineBorder;

import org.apache.log4j.Logger;

import persistence.AppParameters;
import persistence.DatabaseRegion;
import security.DecryptPassword;
import tools.CommonCompareUtil;
import tools.DB2Connection;
import tools.GPConnection;
import enums.DatabaseRegions;

/**
 * Log in frame for the DB2 and Greenplum Comparison. 
 * 
 * @author vivek.subedi
 *
 */
public class DB2GPLogInFrame extends JPanel{

	private static final long serialVersionUID = -662188793021885432L;
	
	private static Logger logger = Logger.getLogger(DB2GPLogInFrame.class);
	
	private JFrame frame;
	private final DB2GPLogInFrame thisFrame;
	public static String greenplumString;
	public static String db2Username;
	public static String db2Password;
	public static String gpUserName;
	public static String gpPassword;
	private Component[] components;
	private String coreTaString;
	private JComboBox<Object> regionComboBox;
	private String filedb2Username;
	private String filedb2Password;
	private String filegpPreprodUsername;
	private String filegpPreprodPassword;
	private String filegpProdUsername;
	private String filegpProdPassword;
	private DatabaseRegion databaseRegion;
	
	public DB2GPLogInFrame(final String coreTaString) {
		
		thisFrame = this;
		setLayout(null);
		this.coreTaString = coreTaString;
		
		//Initializing database drivers
		initializeDatabaseConnections();
		
		//Default panel to display
		JPanel panel = new JPanel();
		panel.setLayout(null);
		panel.setBounds(0, 0, 794, 292);
		panel.setBorder(new LineBorder(Color.RED));
		add(panel);
		
		JLabel environmentLabel = new JLabel("Environment:");
		environmentLabel.setBounds(100, 50, 120, 30);
		environmentLabel.setFont(new Font("New Times Roman", Font.BOLD, 18));
		panel.add(environmentLabel);
		
		regionComboBox = new JComboBox<Object>(DatabaseRegions.values());
		regionComboBox.setFont(new Font("New Times Roman",Font.BOLD,16));
		regionComboBox.setBounds(250, 45, 150, 40);
		panel.add(regionComboBox);
		
		JButton logInInButton = new JButton("Log In");
		logInInButton.setFont(new Font("New Times Roman",Font.BOLD,22));
		logInInButton.setBounds(350, 150, 100, 50);
		panel.add(logInInButton);
		
		components = new Component[] {regionComboBox, logInInButton};
		
		//Default button to activate when user enters Enter Key
		//contentPanel.getRootPane().setDefaultButton(logInInButton);
		
		logInInButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							Properties config = new Properties();
							
							//loading database username and password to run the application
							InputStream in = new FileInputStream("checksumdb.properties");
							//InputStream in = this.getClass().getClassLoader().getResourceAsStream("checksumdb.properties");
							config.load(in);
							DecryptPassword decryptPassword = new DecryptPassword("checksumdb.properties");
							filedb2Username = config.getProperty("db2.username");
							filedb2Password = decryptPassword.decryptPropertyValue("db2.password");
							filegpPreprodUsername = config.getProperty("gp.preprod.username");
							filegpPreprodPassword = decryptPassword.decryptPropertyValue("gp.preprod.password");
							filegpProdUsername = config.getProperty("gp.prod.username");
							filegpProdPassword = decryptPassword.decryptPropertyValue("gp.prod.password");
							
							//loading database connection string and other config values
							InputStream configInput = new FileInputStream("checksumconfig.properties");
							//InputStream configInput = this.getClass().getClassLoader().getResourceAsStream("checksumconfig.properties");
							config.load(configInput);
							String connectionString = config.getProperty(regionComboBox.getSelectedItem().toString());
							String[] parts = connectionString.split(",");
							if (parts.length != 8) {
								JOptionPane.showMessageDialog(frame, "There is not all the property set in the file. Please check it again !", "All required Properties are not in file !", 
										JOptionPane.WARNING_MESSAGE);
								return;
							} else {
								databaseRegion = new DatabaseRegion();
								databaseRegion.setCoreConnectionString(parts[0].trim());
								databaseRegion.setTaConnectionString(parts[1].trim());
								databaseRegion.setGreenplumConnectionString(parts[2].trim());
								databaseRegion.setNonProdOrProdString(parts[3].trim());
								databaseRegion.setDisplay(parts[4].trim());
								databaseRegion.setCore(parts[5].trim());
								databaseRegion.setTa(parts[6].trim());
								databaseRegion.setRegion(parts[7].trim());
							}
						} catch (Exception e) {
							logger.info("Can not read the properties file. Please make sure the properties file in right place and in right format !");
							new IllegalArgumentException("Can not read the properties file. Please make sure the properties file in right place and in right format !");
							return;
						}
						disableComponents();
						if (databaseRegion.getDisplay().equals("P2396")) {
							if (filegpProdUsername.trim() == null || filegpProdPassword.trim() == null) {
								JOptionPane.showMessageDialog(frame, "Prod Greenplum credentials are missing", "Greenplum credentials is missing !", JOptionPane.WARNING_MESSAGE);
								enableComponents();
								return;
							} 
							else if(filedb2Username.trim() ==  null || filedb2Password.trim() == null)  {
								JOptionPane.showMessageDialog(frame, "DB2 credentials are missing", "DB2 credentials is missing !", JOptionPane.WARNING_MESSAGE);
								enableComponents();
								return;
							} else {
								if (!DB2Connection.testConnection(databaseRegion, filedb2Username.trim(), filedb2Password.trim())) {
									JOptionPane.showMessageDialog(frame, "Error!  Can not connect to DB2 database, check your username and password.", "DB2 Connection Issue", JOptionPane.ERROR_MESSAGE);		
									enableComponents();
									return;
								} else {
									db2Username = filedb2Username.trim();
									db2Password = filedb2Password.trim();
								}
								
								if (!GPConnection.testConnection(databaseRegion, filegpProdUsername.trim(), filegpProdPassword.trim())) {
									JOptionPane.showMessageDialog(frame, "Error!  Can not connect to Greenplum database, check your username and password.", "Greenplum Connection Issue", JOptionPane.ERROR_MESSAGE);		
									enableComponents();
									return;
								} else {
									gpUserName = filegpProdUsername.trim();
									gpPassword = filegpProdPassword.trim();
								}
							}
						}
						
						//checks for all regions except production
						if (!databaseRegion.getDisplay().equals("P2396")) {
							if (filedb2Username.trim() == null || filedb2Password.trim() == null) {
								JOptionPane.showMessageDialog(frame, "DB2 credentials are missing", "DB2 credentials is missing !", JOptionPane.WARNING_MESSAGE);
								enableComponents();
								return;
							} 
							else if(filegpPreprodUsername.trim() == null || filegpPreprodPassword.trim() == null){
								JOptionPane.showMessageDialog(frame, "Pre-prod Greenplum credentials are missing", "Greenplum credentials is missing !", JOptionPane.WARNING_MESSAGE);
								enableComponents();
								return;
							} else {
								if (!DB2Connection.testConnection(databaseRegion, filedb2Username.trim(), filedb2Password.trim())) {
									JOptionPane.showMessageDialog(frame, "Error!  Can not connect to DB2 database, check your username and password.", "DB2 Connection Issue", JOptionPane.ERROR_MESSAGE);		
									enableComponents();
									return;
								} else {
									db2Username = filedb2Username.trim();
									db2Password = filedb2Password.trim();
								}
								
								if (!GPConnection.testConnection(databaseRegion, filegpPreprodUsername.trim(), filegpPreprodPassword.trim())) {
									JOptionPane.showMessageDialog(frame, "Error!  Can not connect to Greenplum database, check your username and password.", "Greenplum Connection Issue", JOptionPane.ERROR_MESSAGE);		
									enableComponents();
									return;
								} else {
									gpUserName = filegpPreprodUsername.trim();
									gpPassword = filegpPreprodPassword.trim();
								}
							}
						}

						AppParameters parameters = new AppParameters();
						parameters.setDb2Username(db2Username);
						parameters.setDb2Password(db2Password);
						parameters.setGpUsername(gpUserName);
						parameters.setGpPassword(gpPassword);
						parameters.setRegion(databaseRegion);
						parameters.setComparisonType(CommonCompareUtil.CORETA);
						
						DisplayTableFrame displayTableFrame = null;
						ArchiveDisplayTableFrame archiveDisplayTableFrame = null;
						if (coreTaString.equalsIgnoreCase(CommonCompareUtil.TAGP)) {
							parameters.setComparisonType(CommonCompareUtil.TAGP);
							displayTableFrame = new DisplayTableFrame(parameters);
							displayFrame(displayTableFrame);
						} else if (coreTaString.equalsIgnoreCase(CommonCompareUtil.ARCHIVE)) {
							parameters.setComparisonType(CommonCompareUtil.ARCHIVE);
							archiveDisplayTableFrame = new ArchiveDisplayTableFrame(parameters);
							displayArchiveFrame(archiveDisplayTableFrame);
						}
						else {
							parameters.setComparisonType(CommonCompareUtil.COREGP);
							displayTableFrame = new DisplayTableFrame(parameters);
							displayFrame(displayTableFrame);
						}
						
						enableComponents();
					}
				}).start(); 
				
			}
		});
	}
	
	/**
	 * @param archiveDisplayTableFrame
	 */
	protected void displayArchiveFrame(ArchiveDisplayTableFrame archiveDisplayTableFrame) {
		
		if (ArchiveDisplayTableFrame.tableList.isEmpty()) {
			JOptionPane.showMessageDialog(frame, "There are not any archive tables in DB2","No Tables ["+databaseRegion.getDisplay()+"] !", JOptionPane.INFORMATION_MESSAGE);
			enableComponents();
			return;
		}
		
		archiveDisplayTableFrame.setTitle("Archived tables in DB2 of ["+databaseRegion.getDisplay()+"] region!!");
		thisFrame.setVisible(false);
		archiveDisplayTableFrame.setVisible(true);
		
	}

	/**
	 * @param displayTableFrame 
	 * 
	 */
	protected void displayFrame(DisplayTableFrame displayTableFrame) {
		
		if (DisplayTableFrame.tableList.isEmpty()) {
			JOptionPane.showMessageDialog(frame, "There are not any tables that is replicated each night","No Tables ["+databaseRegion.getDisplay()+"] !", JOptionPane.INFORMATION_MESSAGE);
			enableComponents();
			return;
		}
		
		displayTableFrame.setTitle("Replicated tables to Greenplum from DB2 region ["+databaseRegion.getDisplay()+"] of "+coreTaString.trim()+"!!");
		thisFrame.setVisible(false);
		displayTableFrame.setVisible(true);
	}

	/**
	 * Initialize the Database connections with a JDBC drivers
	 */
	public static void initializeDatabaseConnections() {
		try {
			Class.forName(CommonCompareUtil.DB2JDBCDRIVER);
			Class.forName(CommonCompareUtil.GPJDBCDRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void disableComponents() {
		for(Component c : components) {
			c.setEnabled(false);
		}
	}
	
	private void enableComponents() {
		for(Component c : components) {
			c.setEnabled(true);
		}
	}
	
	public void clearAll() {
		regionComboBox.setSelectedIndex(0);
	}

}
