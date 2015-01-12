/**
 * 
 */
package gui;

import java.awt.BorderLayout;
import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JTabbedPane;
import javax.swing.UIDefaults;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import tools.CommonCompareUtil;

/**
 * @author vivek.subedi
 *
 */
public class DataUtilFrame{
	
	private JTabbedPane tabbedPane;
	private JFrame frame;

	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				UIDefaults defaults = null;
				try {
				    for (LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
				        if ("Nimbus".equals(info.getName())) {
				            UIManager.setLookAndFeel(info.getClassName());
				            defaults = UIManager.getLookAndFeelDefaults();
				            defaults.put("nimbusOrange",defaults.get("nimbusBase"));
				            break;
				        }
				    }
				    DataUtilFrame window = new DataUtilFrame();
					window.frame.setVisible(true);
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

	}
	
	public DataUtilFrame() {
		//frame for window
		frame = new JFrame();
		frame.setBounds(300, 100, 800, 350);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setResizable(false);
		frame.setTitle("Please choose your Comparison Option !");
		
		tabbedPane = new JTabbedPane(JTabbedPane.TOP);
		frame.getContentPane().add(tabbedPane, BorderLayout.CENTER);
		
		//All tabs
		//core to ta login panel
		final CoreTALogInFrame coreTALogInFrame = new CoreTALogInFrame();
		coreTALogInFrame.setName("CORE to TOTAL ACCESS");
		tabbedPane.add(coreTALogInFrame, 0);
		tabbedPane.setSelectedIndex(0);
		
		//db2 core to gp login panel
		final DB2GPLogInFrame coreGPLogInFrame = new DB2GPLogInFrame(CommonCompareUtil.COREGP);
		coreGPLogInFrame.setName("CORE to GREENPLUM");
		tabbedPane.add(coreGPLogInFrame, 1);
		
		//db2 ta to gp login panel
		final DB2GPLogInFrame taGPLogInFrame = new DB2GPLogInFrame(CommonCompareUtil.TAGP);
		taGPLogInFrame.setName("TOTAL ACCESS to GREENPLUM");
		tabbedPane.add(taGPLogInFrame, 2);
		
		//db2 ta to gp login panel
		final DB2GPLogInFrame taArchiveGPArchiveLogInFrame = new DB2GPLogInFrame(CommonCompareUtil.ARCHIVE);
		taArchiveGPArchiveLogInFrame.setName("DB2 ARCHIVE to GREENPLUM ARCHIVE");
		tabbedPane.add(taArchiveGPArchiveLogInFrame, 3);
		
		//Tabbed Pane State Change Listener
		tabbedPane.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent e) {
				tabbedPane.getSelectedComponent();
				if ("CORE to TOTAL ACCESS".equalsIgnoreCase(tabbedPane.getSelectedComponent().getName())) {
					coreTALogInFrame.clearAll();
				} else {
					coreGPLogInFrame.clearAll();
					taGPLogInFrame.clearAll();
					taArchiveGPArchiveLogInFrame.clearAll();
				}
			}
	    });
	}
}
