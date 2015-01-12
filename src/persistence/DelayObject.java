/**
 * 
 */
package persistence;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * The persistence class to create DelayObject. This class implements @Delayed interface so that we can put in @DelayQueue.
 * This object can't be touched by any threads unless the time expires that we set. Once the time expires, consumer threads picks it up to process
 * 
 * @author vivek.subedi
 *
 */
public class DelayObject implements Delayed {
	
	private String tablename;
	private Map<String, LinkedList<String>> sumColumnsMap;
	private Map<String, LinkedList<String>> primaryKeyMap;
	private Map<String, Map<String, SumObject>> coreTaSideSumObjectsMap;
	private Map<String, Map<String, SumObject>> taGpSideSumObjectsMap;
	private Long startTime;
	

	/**
	 * Instantiates a new delay object.
	 *
	 * @param tablename - the tablename
	 * @param sumColumnsMap - @HashMap of the tables and their summable columns
	 * @param primaryKeyMap - @HashMap of the tables and their primary keys
	 * @param delay - the expire time i.e how long should this object shouldn't be touch by any threads
	 */
	public DelayObject(String tablename, Map<String, LinkedList<String>> sumColumnsMap, Map<String, LinkedList<String>> primaryKeyMap, Long delay) {
		this.tablename = tablename;
		this.sumColumnsMap = sumColumnsMap;
		this.primaryKeyMap = primaryKeyMap;
		this.startTime = System.currentTimeMillis() + delay;
	}
	
	public DelayObject(String tablename) {
		super();
		this.tablename = tablename;
	}

	@Override
	public int compareTo(Delayed o) {
		
		if (this.startTime < ((DelayObject) o).startTime) {
			return -1;
		}
		if (this.startTime >  ((DelayObject) o).startTime) {
			return 1;
		}
		return 0;
	}


	@Override
	public long getDelay(TimeUnit unit) {
		Long diff = startTime - System.currentTimeMillis();
		return unit.convert(diff, TimeUnit.MILLISECONDS);
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public Map<String, LinkedList<String>> getSumColumnsMap() {
		return sumColumnsMap;
	}

	public void setSumColumnsMap(Map<String, LinkedList<String>> sumColumnsMap) {
		this.sumColumnsMap = sumColumnsMap;
	}

	public Map<String, LinkedList<String>> getPrimaryKeyMap() {
		return primaryKeyMap;
	}

	public void setPrimaryKeyMap(Map<String, LinkedList<String>> primaryKeyMap) {
		this.primaryKeyMap = primaryKeyMap;
	}

	public Map<String, Map<String, SumObject>> getCoreSideSumObjectMap() {
		return coreTaSideSumObjectsMap;
	}

	public void setCoreSideSumObjectMap(
			Map<String, Map<String, SumObject>> coreSideSumObjectMap) {
		this.coreTaSideSumObjectsMap = coreSideSumObjectMap;
	}

	public Map<String, Map<String, SumObject>> getTaSideSumObjectMap() {
		return taGpSideSumObjectsMap;
	}

	public void setTaSideSumObjectMap(
			Map<String, Map<String, SumObject>> taSideSumObjectMap) {
		this.taGpSideSumObjectsMap = taSideSumObjectMap;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((coreTaSideSumObjectsMap == null) ? 0 : coreTaSideSumObjectsMap
						.hashCode());
		result = prime * result
				+ ((primaryKeyMap == null) ? 0 : primaryKeyMap.hashCode());
		result = prime * result
				+ ((startTime == null) ? 0 : startTime.hashCode());
		result = prime * result
				+ ((sumColumnsMap == null) ? 0 : sumColumnsMap.hashCode());
		result = prime
				* result
				+ ((taGpSideSumObjectsMap == null) ? 0 : taGpSideSumObjectsMap
						.hashCode());
		result = prime * result
				+ ((tablename == null) ? 0 : tablename.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DelayObject other = (DelayObject) obj;
		if (coreTaSideSumObjectsMap == null) {
			if (other.coreTaSideSumObjectsMap != null)
				return false;
		} else if (!coreTaSideSumObjectsMap.equals(other.coreTaSideSumObjectsMap))
			return false;
		if (primaryKeyMap == null) {
			if (other.primaryKeyMap != null)
				return false;
		} else if (!primaryKeyMap.equals(other.primaryKeyMap))
			return false;
		if (startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!startTime.equals(other.startTime))
			return false;
		if (sumColumnsMap == null) {
			if (other.sumColumnsMap != null)
				return false;
		} else if (!sumColumnsMap.equals(other.sumColumnsMap))
			return false;
		if (taGpSideSumObjectsMap == null) {
			if (other.taGpSideSumObjectsMap != null)
				return false;
		} else if (!taGpSideSumObjectsMap.equals(other.taGpSideSumObjectsMap))
			return false;
		if (tablename == null) {
			if (other.tablename != null)
				return false;
		} else if (!tablename.equals(other.tablename))
			return false;
		return true;
	}
	
	
}
