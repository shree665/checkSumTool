/**
 * 
 */
package persistence;

import tools.CommonCompareUtil;

/**
 * @author vivek.subedi
 *
 */
public class DatabaseRegion {
	
	private String coreConnectionString;
	private String taConnectionString;
	private String greenplumConnectionString;
	private String nonProdOrProdString;
	private String display;
	private String core;
	private String ta;
	private String region;
	
	
	public DatabaseRegion() {
		super();
	}

	public DatabaseRegion(String coreConnectionString, String taConnectionString, String greenplumConnectionString, String nonProdOrProdString, String display, String core, String ta, String region) {
		this.coreConnectionString = coreConnectionString;
		this.taConnectionString = taConnectionString;
		this.greenplumConnectionString = greenplumConnectionString;
		this.nonProdOrProdString = nonProdOrProdString;
		this.display = display;
		this.core = core;
		this.ta = ta;
		this.region = region;
	}

	public String getCoreConnectionString() {
		return ("jdbc:db2://" + coreConnectionString);
	}


	public void setCoreConnectionString(String coreConnectionString) {
		this.coreConnectionString = coreConnectionString;
	}


	public String getTaConnectionString() {
		return ("jdbc:db2://" + taConnectionString);
	}


	public void setTaConnectionString(String taConnectionString) {
		this.taConnectionString = taConnectionString;
	}


	public String getGreenplumConnectionString() {
		return (CommonCompareUtil.GPCONNECTIONURL +greenplumConnectionString);
	}


	public void setGreenplumConnectionString(String greenplumConnectionString) {
		this.greenplumConnectionString = greenplumConnectionString;
	}


	public String getNonProdOrProdString() {
		return nonProdOrProdString;
	}


	public void setNonProdOrProdString(String nonProdOrProdString) {
		this.nonProdOrProdString = nonProdOrProdString;
	}


	public String getDisplay() {
		return display;
	}


	public void setDisplay(String display) {
		this.display = display;
	}


	public String getCore() {
		return core;
	}


	public void setCore(String core) {
		this.core = core;
	}


	public String getTa() {
		return ta;
	}


	public void setTa(String ta) {
		this.ta = ta;
	}


	public String getRegion() {
		return region;
	}


	public void setRegion(String region) {
		this.region = region;
	}
	
	
}
