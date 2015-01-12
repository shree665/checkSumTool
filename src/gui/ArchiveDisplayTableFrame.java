/**
 * 
 */
package gui;

import java.awt.Component;
import java.awt.Desktop;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.ListModel;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.border.BevelBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.log4j.Logger;

import concurrency.WorkerThread;
import persistence.AppParameters;
import persistence.DelayObject;
import persistence.DifferenceObject;
import persistence.SumObject;
import service.CoreTAService;
import tools.CheckSumSQLGenerator;
import tools.CommonCompareUtil;
import email.EmailAttachmentSender;
import excelfactory.ExcelReportFactory;

/**
 * @author vivek.subedi
 *
 */
public class ArchiveDisplayTableFrame extends JFrame{

	private static final long serialVersionUID = 1L;
	
	private static Logger logger = Logger.getLogger(ArchiveDisplayTableFrame.class);
	
	private final ArchiveDisplayTableFrame thisFrame;
	private JFrame frame;
	private JPanel contentPanel;
	private LinkedList<String> orderedRepTableList;
	private DefaultListModel<Object> repModel;
	private DefaultListModel<Object> addedRepModel;
	private JTextField searchField;
	private static CoreTAService coreTAService;
	private Component[] components;
	private Map<String, LinkedList<String>> sumColumnsMap;
	private LinkedHashSet<String> coreTables;
	protected Map<String, Map<String, SumObject>> coreSideSumObjectMap;
	protected Map<String, Map<String, SumObject>> taSideSumObjectMap;
	public static Integer delayTime;
	private JProgressBar progressBar;
	protected static LinkedList<String> tableList;
	public static LinkedList<String> noSumColumnsTable;
	public static String compareType;
	private Boolean commandline = false;
	private Integer numDiffTables;
	private File saveFile;
	private Vector<DelayObject> allQueryResultVector;
	
	public ArchiveDisplayTableFrame(final AppParameters parameters) {
		//Applying shutdown hook in the application
				Runtime.getRuntime().addShutdownHook(new Thread() {
					@Override
					public void run() {
						logger.info("Application is closing. Dumping all the data into the excel file. It might take some time");
						Map<String, LinkedList<DifferenceObject>> differencesMap = CommonCompareUtil.findDifferences(allQueryResultVector, parameters);
						//Writes all the value of resultset of CORE and TA to the Excel file
						if (!differencesMap.isEmpty()) {
							ExcelReportFactory.generateReport(differencesMap, saveFile, compareType);
							logger.info(saveFile.getName()+" is successfully created in ["+saveFile.getPath()+"] directory");
						} else {
							JOptionPane.showMessageDialog(frame,"There are not anything to write into excel file", "Nothing to write!!!",JOptionPane.INFORMATION_MESSAGE);
							logger.info("There is not anything to write....");
							return;
						}
						logger.info("Application is Closing...");
					}
				});
		thisFrame = this;
		compareType = parameters.getComparisonType();
		initilizeService(parameters);
		
		//frame to display
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(120, 50, 820, 780);
		contentPanel = new JPanel();
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPanel);
		contentPanel.setLayout(null);
		this.setResizable(false);
		
		//textField to enter search character of a table
		searchField = new JTextField();
		searchField.setBounds(5, 40, 300, 28);
		contentPanel.add(searchField);
		searchField.setColumns(10);
		
		//Choice for Thread Pool
		JLabel threadPoolLabel = new JLabel("Max # of threads: ");
		threadPoolLabel.setBounds(400, 20, 100, 30);
		contentPanel.add(threadPoolLabel);
		final JComboBox<Object> threadPoolComboBox = new JComboBox<Object>(CheckSumSQLGenerator.THREADS);
		threadPoolComboBox.setBounds(500,20,80,30);
		threadPoolComboBox.setSelectedIndex(3);
		contentPanel.add(threadPoolComboBox);
		
		//Scrollpane to display tables that are used by the rep server
		JScrollPane scrollPane = new JScrollPane();
		scrollPane.setBounds(5, 80, 300, 520);
		contentPanel.add(scrollPane);
		repModel = new DefaultListModel<Object>();
		final JList<Object> repTableExplorer = new JList<Object>(repModel);
		repTableExplorer.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		repTableExplorer.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		scrollPane.setViewportView(repTableExplorer);
		
		//check box to compare all tables
		final JCheckBox checkBox = new JCheckBox("Select All Tables to Compare");
		checkBox.setBounds(10, 600, 200, 50);
		checkBox.setFont(new Font("Sarif", Font.BOLD, 12));
		contentPanel.add(checkBox);
		
		//Contacting Database to get all the archive tables of archive schema tables
		try {
			coreTAService.archiveTableNames(parameters.getRegion());
		} catch (SQLException e1) {
			logger.info(CommonCompareUtil.getStackTrace(e1));
			enableComponents();
			return;
		}
		tableList = coreTAService.getTableNameList();
		
		Collections.sort(tableList);
		orderedRepTableList = tableList;
		
		//adding all the tables to the list
		Integer i = 0;
		for (String string : tableList) {
			repModel.addElement(string.trim());
			i++;
		}
		logger.info("Number of Tables that are being replicate each night: "+ i);
		
		//key listener to search the table names from the list
		searchField.addKeyListener(new KeyListener() {
			@Override
			public void keyTyped(KeyEvent e) {}
			
			@Override
			public void keyReleased(KeyEvent e) {
				String text = searchField.getText().toUpperCase().trim();
				repModel.clear();
				if (text.isEmpty()) {
					for (String string : orderedRepTableList) {
						repModel.addElement(string.toUpperCase().trim());
						continue;
					}
					return;
				} else {
					for (String string : orderedRepTableList) {
						if (string.toString().contains(text)) {
							repModel.addElement(string.toUpperCase().trim());
						}
					}
				}
			}
			
			@Override
			public void keyPressed(KeyEvent arg0) {}
		});
		
		JLabel db2SearchLbl = new JLabel("Search Table name:");
		db2SearchLbl.setFont(new Font("Sarif", Font.BOLD, 16));
		db2SearchLbl.setBounds(6, 12, 261, 16);
		contentPanel.add(db2SearchLbl);
		
		JButton addButton = new JButton("Add >>");
		addButton.setBounds(320, 300, 100, 50);
		addButton.setFont(new Font("Sarif", Font.BOLD, 16));
		contentPanel.add(addButton);
		addButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				if (repTableExplorer.isSelectionEmpty()) {
					JOptionPane.showMessageDialog(frame, "Please select tables from the table list on the left to add to the list",
							"Table is not selected !", JOptionPane.INFORMATION_MESSAGE);
					repTableExplorer.requestFocus();
					return;
				} else {
						List<Object> selectedList = new ArrayList<Object>();
						selectedList = repTableExplorer.getSelectedValuesList();
						for (Object selectedValue : selectedList) {
							addedRepModel.addElement(selectedValue.toString().trim());
							repModel.removeElement(selectedValue);
							orderedRepTableList.remove(selectedValue.toString().trim());
						}
				}			
			}
		});
		
		JLabel addedScrollPanelLabel = new JLabel("Tables that are being compare:");
		addedScrollPanelLabel.setBounds(435, 50, 200, 40);
		contentPanel.add(addedScrollPanelLabel);
		
		//ScrollPane to display those tables that are added to compare
		JScrollPane addedScrollPane = new JScrollPane();
		addedScrollPane.setBounds(435, 80, 300, 520);
		contentPanel.add(addedScrollPane);
		addedRepModel = new DefaultListModel<Object>();
		final JList<Object> addedRepTableExplorer = new JList<Object>(addedRepModel);
		addedRepTableExplorer.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		addedRepTableExplorer.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		addedScrollPane.setViewportView(addedRepTableExplorer);
		
		//Check box linstener to add all the table to the to be process window and the staging window
		checkBox.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				int a = 0;
				if (checkBox.isSelected()) {
					ListModel<Object> repTables = repTableExplorer.getModel();
					LinkedList<String> repTablesLinkedList = new LinkedList<String>();
					for(int i=0; i < repTables.getSize(); i++) {
						repTablesLinkedList.add(repTables.getElementAt(i).toString());
					}
					
					Collections.sort(repTablesLinkedList);
					for (String string : repTablesLinkedList) {
						addedRepModel.addElement(string.trim());
						a++;
					}
					repModel.removeAllElements();
					logger.info("Number of Elements added to the AddedRepModel: "+a);
				} else {
					if (!addedRepModel.isEmpty()) {
						ListModel<Object> addedRepTables = addedRepTableExplorer.getModel();
						LinkedList<String> repTablesLinkedList = new LinkedList<String>();
						for(int i=0; i < addedRepTables.getSize(); i++) {
							repTablesLinkedList.add(addedRepTables.getElementAt(i).toString().replace(CheckSumSQLGenerator.DONE, ""));
						}
						
						Collections.sort(repTablesLinkedList);
						for (String string : repTablesLinkedList) {
							repModel.addElement(string.trim());
							a++;
						}
					}
					addedRepModel.removeAllElements();
					logger.debug("Number of Elements added to the RepModel: "+a);
				}
			}
		});
		
		
		JButton removeButton = new JButton("Remove");
		removeButton.setBounds(460, 600, 100, 30);
		removeButton.setFont(new Font("Sarif", Font.BOLD, 12));
		contentPanel.add(removeButton);
		removeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (!addedRepTableExplorer.isSelectionEmpty()) {
					
					//@LinkedList to hold the rep table name to sort everytime when user remove it from the addedRepmodel to add to the repModel
					LinkedList<String> repTableList = new LinkedList<String>();
					for (int j = 0; j < repModel.size(); j++) {
						repTableList.add(repModel.get(j).toString());
					}
				
					repTableList.add(addedRepTableExplorer.getSelectedValue().toString().replace(CheckSumSQLGenerator.DONE, ""));
					orderedRepTableList.add(addedRepTableExplorer.getSelectedValue().toString().replace(CheckSumSQLGenerator.DONE, ""));
					
					Collections.sort(orderedRepTableList);
					Collections.sort(repTableList);
					repModel.clear();
					for (String string : repTableList) {
						repModel.addElement(string.trim());
					}
					addedRepModel.removeElement(addedRepTableExplorer.getSelectedValue());
				} else {
					JOptionPane.showMessageDialog(frame,"Please select table name from the right box and press remove button","You haven't selected any table", JOptionPane.INFORMATION_MESSAGE);
					addedRepTableExplorer.requestFocus();
					return;
				}
			}
		});
		
		//clears all the model that user added into the model and adds back to the repTables model
		JButton clearAll = new JButton("Clear All");
		clearAll.setBounds(600, 600, 100, 30);
		clearAll.setFont(new Font("Sarif", Font.BOLD, 12));
		contentPanel.add(clearAll);
		clearAll.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				
				//@LinkedList to hold the rep table name to sort everytime when user clear all the tables from the addedRepmodel to add to the repModel
				LinkedList<String> repTableList = new LinkedList<String>();
				for (int j = 0; j < repModel.size(); j++) {
					repTableList.add(repModel.get(j).toString());
				}
				
				//storing all the tables names to the templist to add back into the repModel
				LinkedList<String> tempList = new LinkedList<String>();
				for (int j = 0; j < addedRepModel.getSize(); j++) {
					tempList.add(addedRepModel.get(j).toString().replace(CheckSumSQLGenerator.DONE, ""));
				}

				repTableList.addAll(tempList);
				orderedRepTableList.addAll(tempList);
				Collections.sort(orderedRepTableList);
				Collections.sort(repTableList);
				repModel.clear();
				for (String string : repTableList) {
					repModel.addElement(string.trim());
				}
				addedRepModel.clear();
			}
		});
		
		//Run Comparison Button
		JButton runComparisonButton = new JButton("Run Comparison");
		runComparisonButton.setFont(new Font("New Times Roman",Font.BOLD,22));
		runComparisonButton.setBounds(600, 640, 200, 50);
		contentPanel.add(runComparisonButton);
		
		components = new Component[] {searchField, scrollPane, repTableExplorer, addedScrollPane, addedRepTableExplorer, addButton, removeButton, clearAll
				, runComparisonButton, checkBox, threadPoolComboBox};
		
		//specifying the default button
		thisFrame.getRootPane().setDefaultButton(runComparisonButton);
		
		runComparisonButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				new Thread(new Runnable() {
					@Override
					public void run() {
						disableComponents();
						Long startTime = System.currentTimeMillis();
						noSumColumnsTable = new LinkedList<String>();
						
						//checks for the tables that user added it into the model or not
						if (addedRepModel.isEmpty()) {
							JOptionPane.showMessageDialog(frame, "Please add tables to the list that you want to compare", "Table name is missing !", 
									JOptionPane.WARNING_MESSAGE);
							enableComponents();
							return;
						} else {
							
							//trimming the table name that are executed before i.e. "ADDRESS  ----  Done in CORE !" to ADDRESS only if the table is already in
							//to be process box
							LinkedList<String> coreList = new LinkedList<String>();
							for (int i = 0; i < addedRepModel.getSize(); i++) {
								coreList.add(addedRepModel.get(i).toString().replace(CheckSumSQLGenerator.DONE, ""));
							}
							Collections.sort(coreList);
							addedRepModel.clear();
							for (String string : coreList) {
								addedRepModel.addElement(string.trim());
							}

							//Displaying the file chooser for the name so that we can save the ouput when comparison happens
							SwingUtilities.invokeLater(new Runnable() {
								@Override
								public void run() {
									progressBar.setValue(0);
									progressBar.setString("Choosing File Name...");
									progressBar.setVisible(true);
									progressBar.setEnabled(true);
								}
							});
							
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HHmm");
							String fileName = "Report of ["+parameters.getRegion()+"] " + dateFormat.format(Calendar.getInstance().getTime())+".xlsx";
							
							
							JFileChooser chooser = new JFileChooser();
							chooser.setDialogTitle("Where do you want to save the excel File?");
							chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
							chooser.setSelectedFile(new File(fileName));
							FileNameExtensionFilter filter = new FileNameExtensionFilter("Excel Files(*.xlsx","xlsx");
							chooser.setFileFilter(filter);
							if (chooser.showSaveDialog(thisFrame) != JFileChooser.APPROVE_OPTION) {
								SwingUtilities.invokeLater(new Runnable() {
									@Override
									public void run() {
										progressBar.setValue(0);
										progressBar.setVisible(false);
									}
								});
								enableComponents();
								return;
							}
							
							File saveFile = chooser.getSelectedFile();
							
							//Checking for the file extension. If user do not chooses anything, default is .xlsx
							if (!saveFile.getName().toLowerCase().endsWith(".xlsx")) {
								saveFile = new File(saveFile.getAbsolutePath()+".xlsx");
								logger.debug(saveFile.getPath());
							}
							
							//Starts processing the user selected tables which is from to be process box i.e. added mofel
							ListModel<Object> selectedTables = addedRepTableExplorer.getModel();
							logger.debug("Following Tables are being compare:\n");
							LinkedList<String> selectedTableList = new LinkedList<String>();
							for (int j = 0; j < selectedTables.getSize(); j++) {
								selectedTableList.add(selectedTables.getElementAt(j).toString());
								logger.debug(selectedTables.getElementAt(j));
							}
							logger.info("# of tables those are selected by user: ["+selectedTables.getSize()+"]");
							SwingUtilities.invokeLater(new Runnable() {
								@Override
								public void run() {
									progressBar.setValue(0);
									progressBar.setString("Retrieving columns that needs to be sum up...");
									progressBar.setVisible(true);
								}
							});
							coreTables = new LinkedHashSet<String>();
							
							/**
							 * getting all the columns of tables that can be summed and catching nullpointer exception if the table don't have any columns
							 * that needs to be sum up
							 */
							sumColumnsMap = CommonCompareUtil.getArchiveSumColumnMap(selectedTableList, coreTAService, parameters);
							if (sumColumnsMap == null) {
								logger.debug("The table you are trying to compare doesn't have any columns that can be summed. We will just count table");
								progressBar.setValue(0);
								progressBar.setVisible(false);
								enableComponents();
								return;
							}
							
							/**
							 * Adding those tables to the coreTables list. We will use this list to find out the tables which don't have any sum columns in table.
							 * We do only count for those tables
							 */
							Iterator<Entry<String, LinkedList<String>>> iterator = sumColumnsMap.entrySet().iterator();
							while (iterator.hasNext()) {
								Map.Entry<String, LinkedList<String>> entry = iterator.next();
								String tableName = entry.getKey();
								logger.info(tableName);
								coreTables.add(tableName);
							}
							
							//adding those tables that needs to be do count only
							coreTables = CommonCompareUtil.getAllArchiveTablesWithNoSumColumns(selectedTableList, coreTAService, parameters, coreTables);
							
							SwingUtilities.invokeLater(new Runnable() {
								@Override
								public void run() {
									progressBar.setValue(1);
									progressBar.setString("Executing Queries...");
									progressBar.setVisible(true);
								}
							});
							
							/**
							 * creating Queue for Threads. 
							 * MasterQueue - holds all the tables that need to be compare. This queue is linked blocking queue.
							 * allQueryResultVector - allQueryResultVector is a @Vector which holds the final object of core and TA resultsets as a objects
							 */
							
							LinkedBlockingQueue<String> masterQueue = new LinkedBlockingQueue<String>();
							allQueryResultVector = new Vector<DelayObject>();
							
							//putting all tables that are going to be processed into the masterQueue
							for (String string : coreTables) {
								masterQueue.add(string);
							}
							
							logger.info("The size of master queue: ["+masterQueue.size()+"]");
							ExecutorService executors = Executors.newFixedThreadPool(Integer.valueOf((String) threadPoolComboBox.getSelectedItem()));
							logger.info("# of active threads: ["+Integer.valueOf((String) threadPoolComboBox.getSelectedItem())+"]");
							CompletionService<String> pool = new ExecutorCompletionService<String>(executors);
							//WorkerThread[] workers = new WorkerThread[coreTables.size()];
							for (int i = 0; i < coreTables.size(); i++) {
								String tablename = null;
								try {
									tablename = masterQueue.take();
									Runnable worker = new WorkerThread(tablename, parameters, sumColumnsMap.get(tablename), allQueryResultVector, addedRepModel, commandline);
									pool.submit(worker, tablename);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
							logger.info("Finished initializing thread pool !");
							executors.shutdown();
							
							//taking finished job from the query service
							for (int j = 0; j < coreTables.size(); j++) {
								try {
									String result = pool.take().get();
									progressBar.setValue((90 * (j+1))/coreTables.size());
									progressBar.setString("Done Quering Table....["+result+"]");
									progressBar.setVisible(true);
								} catch (InterruptedException e) {
									e.printStackTrace();
								} catch (ExecutionException e) {
									e.printStackTrace();
								}
							}
							
							//waiting all threads to be done before the comparison
							try {
								executors.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
							} catch (InterruptedException  e) {
								logger.info("Thread gets interepted !");
							}
							logger.info("Finished all threads");
							SwingUtilities.invokeLater(new Runnable() {
								@Override
								public void run() {
									progressBar.setValue(90);
									progressBar.setString("Comparing TOTAL ACCESS ARCHIVE and GREENPLUMN ARCHIVE results...");
								}
							});
							
							logger.debug("Excution of Query in TA and GREENPLUM is completed");
							logger.debug("Parent Thread Resume work to compare those resultsets");
							logger.debug("Processing Differences...");
							
							//Compares the core, TA and Greenplum resultsets depending upon the comparison type
							Map<String, LinkedList<DifferenceObject>> differencesMap = CommonCompareUtil.findDifferences(allQueryResultVector, parameters);
							logger.info("Comparison has been completed");
							
							SwingUtilities.invokeLater(new Runnable() {
								@Override
								public void run() {
									progressBar.setValue(95);
									logger.debug("Writing into the excel file");
									progressBar.setString("Writing into Excel File ...");
								}
							});
							logger.debug("Writing into ["+saveFile.getName()+"] file which is located in ["+saveFile.getAbsolutePath()+"]");
							
							//Writes all the value of resultset of CORE and TA to the Excel file
							if (!differencesMap.isEmpty()) {
								numDiffTables = differencesMap.size();
								ExcelReportFactory.generateReport(differencesMap, saveFile, compareType);
								logger.info(saveFile.getName()+" is successfully created in ["+saveFile.getPath()+"] directory");
							} else {
								JOptionPane.showMessageDialog(frame,"There are not anything to write into excel file", "Nothing to write!!!",JOptionPane.INFORMATION_MESSAGE);
								progressBar.setValue(0);
								progressBar.setVisible(false);
								enableComponents();
								logger.info("There is not anything to write....");
								return;
							}
							
							//Sending Email
							EmailAttachmentSender.sendEmail(saveFile, parameters.getRegion());
							//DataToolEmail dataToolEmail = new DataToolEmail();
							//dataToolEmail.createEmail(saveFile, parameters.getRegion());
							logger.info("Email has been sent !!");
							//Opening file in desktop
							final File fileOpen = saveFile;
							SwingUtilities.invokeLater(new Runnable() {
								@Override
								public void run() {
									progressBar.setValue(100);
									progressBar.setString("Done ! File location: "+fileOpen.getAbsolutePath());
									try {
										Desktop.getDesktop().open(fileOpen);
									} catch (IOException e2) {
										e2.getStackTrace();
									}
								}
							});
						}
						if (!CommonCompareUtil.getErrorTables().isEmpty()) {
							logger.debug("["+CommonCompareUtil.getErrorTables().size()+ "] Following Tables done with errors. Please check log for the error:\n");
							for (String tableName : CommonCompareUtil.getErrorTables()) {
								logger.debug("["+tableName+"]");
							}
						}
						
						logger.info("COMPLETED");
						Long endTime = System.currentTimeMillis();
						logger.info("The total time to process["+numDiffTables+"] tables is: ["+DurationFormatUtils.formatDuration((endTime - startTime), "HH:mm:ss")+"]");
						enableComponents();
					}
				}).start();
			}
		});
		progressBar = new JProgressBar();
		progressBar.setBounds(2, 700, 810, 40);
		progressBar.setString("Waiting...");
		progressBar.setStringPainted(true);
		progressBar.setVisible(false);
		progressBar.setMinimum(0);
		progressBar.setMaximum(100);
		contentPanel.add(progressBar);
	}
	
	//Initializing CoreTAService, GPService
	private static void initilizeService(AppParameters parameters) {
		coreTAService = new CoreTAService(parameters.getDb2Username(), parameters.getDb2Password().replaceAll("'", ""), parameters.getRegion());
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

}
