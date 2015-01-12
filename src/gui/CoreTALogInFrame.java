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
import java.io.IOException;
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

import enums.DatabaseRegions;
import persistence.AppParameters;
import persistence.DatabaseRegion;
import security.DecryptPassword;
import tools.CommonCompareUtil;
import tools.DB2Connection;

/**
 * @author vivek.subedi
 *
 */
public class CoreTALogInFrame extends JPanel{

	private static final long serialVersionUID = 4982666812316863462L;

	private static Logger logger = Logger.getLogger(CoreTALogInFrame.class);
	
	private JFrame frame;
	public static String username;
	public static String password;
	private Component[] components;
	private JComboBox<Object> regionComboBox;
	String filedb2username;
	String filedb2password;
	private DatabaseRegion databaseRegion;
	
	public CoreTALogInFrame() {
		setLayout(null);
		
		//Initializing the database connection to test the connection
		initializeDatabaseConnections();

		//Default panel to display
		JPanel panel = new JPanel();
		panel.setLayout(null);
		panel.setBorder(new LineBorder(new Color(0,0,0,0)));
		panel.setBounds(0, 0, 800, 350);
		add(panel);
		
		//log in panel
		JPanel logInJPanel = new JPanel();
		logInJPanel.setLayout(null);
		logInJPanel.setBorder(new LineBorder(Color.RED));
		logInJPanel.setBounds(0, 0, 794, 292);
		panel.add(logInJPanel);
		
	
		JLabel environmentLabel = new JLabel("Environment:");
		environmentLabel.setBounds(100, 50, 120, 30);
		environmentLabel.setFont(new Font("New Times Roman", Font.BOLD, 18));
		logInJPanel.add(environmentLabel);
		
		regionComboBox = new JComboBox<Object>(DatabaseRegions.values());
		regionComboBox.setFont(new Font("New Times Roman",Font.BOLD,16));
		regionComboBox.setBounds(250, 45, 150, 40);
		logInJPanel.add(regionComboBox);
		
		JButton logInInButton = new JButton("Log In");
		logInInButton.setFont(new Font("New Times Roman",Font.BOLD,22));
		logInInButton.setBounds(350, 150, 100, 50);
		logInJPanel.add(logInInButton);
		
		components = new Component[] {regionComboBox, logInInButton};
		
		//Default button to be use when user enters Enter key
		//thisFrame.getRootPane().setDefaultButton(logInInButton);
		
		logInInButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							
							//loading database username and password to run the application
							Properties config = new Properties();
							InputStream in = new FileInputStream("checksumdb.properties");
							//InputStream in = this.getClass().getClassLoader().getResourceAsStream("credential.properties");
							config.load(in);
							DecryptPassword decryptPassword = new DecryptPassword("checksumdb.properties");
							filedb2username = config.getProperty("db2.username");
							filedb2password = decryptPassword.decryptPropertyValue("db2.password");
							
							//loading database connection string and other config values
							InputStream configInput = new FileInputStream("checksumconfig.properties");
							//InputStream configInput = this.getClass().getClassLoader().getResourceAsStream("config.properties");
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
						} catch (IOException e) {
							logger.info("Can not read the properties file !");
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
						disableComponents();
						if (filedb2username.trim() == null || filedb2password.trim() == null) {
							JOptionPane.showMessageDialog(frame, "Please enter your DB2 credentials", "DB2 credentials is missing !", JOptionPane.WARNING_MESSAGE);
							logger.debug("Credentials are missing.");
							enableComponents();
							return;
						} else {
							if (!DB2Connection.testConnection(databaseRegion, filedb2username.trim(), filedb2password.trim())) {
								logger.info("Credentials are not Valid.");
								JOptionPane.showMessageDialog(frame, "Error!  Can not connect to DB2 database, check your username and password.", "DB2 Connection Issue", JOptionPane.ERROR_MESSAGE);		
								enableComponents();
								return;
							}
							
							AppParameters parameters = new AppParameters();
							parameters.setDb2Username(filedb2username.trim());
							parameters.setDb2Password(filedb2password.trim());
							parameters.setRegion(databaseRegion);
							parameters.setComparisonType(CommonCompareUtil.CORETA);
							
							DisplayTableFrame displayTableFrame = new DisplayTableFrame(parameters);
							if (DisplayTableFrame.tableList.isEmpty()) {
								JOptionPane.showMessageDialog(frame, "There are not any tables that is replicated each night","No Tables ["+regionComboBox.getSelectedItem().toString()+"] !", 
										JOptionPane.INFORMATION_MESSAGE);
								logger.debug("There are not any tables that are replicated each night on ["+regionComboBox.getSelectedItem().toString()+"]");
								enableComponents();
								return;
							}
							
							displayTableFrame.setTitle("Replicated tables of region ["+regionComboBox.getSelectedItem().toString()+"] !");
							displayTableFrame.setVisible(true);
						}
						enableComponents();
					}
				}).start();
			}
		});
	}

	/**
	 * Initialize the Database connections with a JDBC drivers
	 */
	public static void initializeDatabaseConnections() {
		try {
			Class.forName(CommonCompareUtil.DB2JDBCDRIVER);
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
