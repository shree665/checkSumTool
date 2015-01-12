/**
 * 
 */
package enums;

import java.sql.Types;

/**
 * @author vivek.subedi
 *
 */
public enum SqlType {
	BIGINT						(Types.BIGINT),
	INT							(Types.INTEGER),
	BLOB						(Types.BLOB),
	CHAR						(Types.CHAR),
	CLOB						(Types.CLOB),
	DATE						(Types.DATE),
	DECIMAL						(Types.DECIMAL),
	NUMERIC						(Types.DECIMAL),
	FLOAT						(Types.FLOAT),
	INTEGER						(Types.INTEGER),
	LONGVAR						(Types.LONGVARCHAR),
	ROWID						(Types.ROWID),
	SMALLINT					(Types.SMALLINT),
	TIME						(Types.TIME),
	TIMESTMP					(Types.TIMESTAMP),
	TIMESTAMP					(Types.TIMESTAMP),	
	VARBIN						(Types.VARBINARY),
	VARCHAR						(Types.VARCHAR),
	CHARACTER_VARYING			(Types.VARCHAR),
	XML							(Types.SQLXML);
	
	private Integer sqlType;
	
	SqlType(Integer sqlType) {
		this.sqlType = sqlType;
	}
	
	public Integer getSqlType() {
		return sqlType;
	}
	
	public void setSqlType(Integer sqlType) {
		this.sqlType = sqlType;
	}
}
