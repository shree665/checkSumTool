/**
 * 
 */
package persistence;

import java.io.Serializable;

import enums.DifferenceType;

/**
 * The Persistence class of Difference object. While comparing, we create object which having following attribute
 * columnName
 * coreTaColumnValue
 * taGpColumnValue
 * DifferenceType
 * DifferBy
 * 
 * @author vivek.subedi
 *
 */
public class DifferenceObject implements Serializable{

	private static final long serialVersionUID = -7096128696594919794L;
	
	private String columnName;
	private Object coreTaColumnName;
	private Object taGpColumnValue;
	private DifferenceType differenceType;
	private Object differBy;
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public Object getCoreColumnValue() {
		return coreTaColumnName;
	}
	public void setCoreColumnValue(Object coreColumnValue) {
		this.coreTaColumnName = coreColumnValue;
	}
	public Object getTaColumnValue() {
		return taGpColumnValue;
	}
	public void setTaColumnValue(Object taColumnValue) {
		this.taGpColumnValue = taColumnValue;
	}
	public DifferenceType getDifferenceType() {
		return differenceType;
	}
	public void setDifferenceType(DifferenceType differenceType) {
		this.differenceType = differenceType;
	}
	public Object getDifferBy() {
		return differBy;
	}
	public void setDifferBy(Object differBy) {
		this.differBy = differBy;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result
				+ ((coreTaColumnName == null) ? 0 : coreTaColumnName.hashCode());
		result = prime * result
				+ ((differBy == null) ? 0 : differBy.hashCode());
		result = prime * result
				+ ((differenceType == null) ? 0 : differenceType.hashCode());
		result = prime * result
				+ ((taGpColumnValue == null) ? 0 : taGpColumnValue.hashCode());
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
		DifferenceObject other = (DifferenceObject) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (coreTaColumnName == null) {
			if (other.coreTaColumnName != null)
				return false;
		} else if (!coreTaColumnName.equals(other.coreTaColumnName))
			return false;
		if (differBy == null) {
			if (other.differBy != null)
				return false;
		} else if (!differBy.equals(other.differBy))
			return false;
		if (differenceType != other.differenceType)
			return false;
		if (taGpColumnValue == null) {
			if (other.taGpColumnValue != null)
				return false;
		} else if (!taGpColumnValue.equals(other.taGpColumnValue))
			return false;
		return true;
	}
	
}
