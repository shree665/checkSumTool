/**
 * 
 */
package persistence;

import com.beust.jcommander.Parameter;

import enums.DatabaseRegions;

/**
 * @author vivek.subedi
 *
 */
public class AppParameters {
	
	@Parameter(names = {"--help","-help","-?"}, help = true, description = "Display usage")
	private boolean displayHelp = false;
	
	private String db2Username;
	private String db2Password;
	private String gpUsername;
	private String gpPassword;
	private String outputDirectoryPath;
	private Integer maxNumberofThread;
	private Integer delayTime;
	private DatabaseRegion region;
	
	@Parameter(names = "-e", required = true, description = "DB2 Database Environment i.e T5775 etc.")
	private DatabaseRegions display;
	
	@Parameter(names = "-ct", required = true, description = "Comparison Type i.e. CORETA, COREGP, and TAGP")
	private String comparisonType;

	public boolean isDisplayHelp() {
		return displayHelp;
	}

	public void setDisplayHelp(boolean displayHelp) {
		this.displayHelp = displayHelp;
	}

	public String getDb2Username() {
		return db2Username;
	}

	public void setDb2Username(String db2Username) {
		this.db2Username = db2Username;
	}

	public String getDb2Password() {
		return db2Password;
	}

	public void setDb2Password(String db2Password) {
		this.db2Password = db2Password;
	}

	public String getGpUsername() {
		return gpUsername;
	}

	public void setGpUsername(String gpUsername) {
		this.gpUsername = gpUsername;
	}

	public String getGpPassword() {
		return gpPassword;
	}

	public void setGpPassword(String gpPassword) {
		this.gpPassword = gpPassword;
	}

	public String getOutputDirectoryPath() {
		return outputDirectoryPath;
	}

	public void setOutputDirectoryPath(String outputDirectoryPath) {
		this.outputDirectoryPath = outputDirectoryPath;
	}

	public Integer getMaxNumberofThread() {
		return maxNumberofThread;
	}

	public void setMaxNumberofThread(Integer maxNumberofThread) {
		this.maxNumberofThread = maxNumberofThread;
	}

	public Integer getDelayTime() {
		return delayTime;
	}

	public void setDelayTime(Integer delayTime) {
		this.delayTime = delayTime;
	}

	public DatabaseRegion getRegion() {
		return region;
	}

	public void setRegion(DatabaseRegion region) {
		this.region = region;
	}

	public DatabaseRegions getDisplay() {
		return display;
	}

	public void setDisplay(DatabaseRegions display) {
		this.display = display;
	}

	public String getComparisonType() {
		return comparisonType;
	}

	public void setComparisonType(String comparisonType) {
		this.comparisonType = comparisonType;
	}

	
}
