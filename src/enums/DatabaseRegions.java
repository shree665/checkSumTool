/**
 * 
 */
package enums;

/**
 * Enum class to hold the connection strings of CORE/TA/GREENPLUM and region names and its value
 * 
 * @author vivek.subedi
 *
 */
public enum DatabaseRegions {
	
	T5775("T5775"), 
	Q5775("Q5775"), 
	U2495("U2495"), 
	U2496("U2496"), 
	U2861("U2861"), 
	U2993("U2993"), 
	V2247("V2247"), 
	V5775("V5775"),
	J2374("J2374"), 
	P2396("P2396");

	private final String display;


	DatabaseRegions(String display) {
		this.display = display;
	}

	public String getDisplay() {
		return display;
	}

}

