/*
 * Copyright 2014 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.databases.definitions;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTReader;
import java.text.SimpleDateFormat;
import java.util.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.internal.oracle.*;
import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 * Defines the features of all Oracle databases that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is sub-classed by {@link Oracle11DBDefinition} and
 * {@link  Oracle12DBDefinition} to provide the full set of features required to
 * use an Oracle database.
 *
 * @author Gregory Graham
 */
public class OracleDBDefinition extends DBDefinition {

	String dateFormatStr = "yyyy-M-d HH:mm:ss.SSS Z";
	String oracleDateFormatStr = "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM";
	SimpleDateFormat javaToStringFormatter = new SimpleDateFormat(dateFormatStr);
	private static final String[] reservedWordsArray = new String[]{"ACCESS", "ACCOUNT", "ACTIVATE", "ADD", "ADMIN", "ADVISE", "AFTER", "ALL", "ALL_ROWS", "ALLOCATE", "ALTER", "ANALYZE", "AND", "ANY", "ARCHIVE", "ARCHIVELOG", "ARRAY", "AS", "ASC", "AT", "AUDIT", "AUTHENTICATED", "AUTHORIZATION", "AUTOEXTEND", "AUTOMATIC", "BACKUP", "BECOME", "BEFORE", "BEGIN", "BETWEEN", "BFILE", "BITMAP", "BLOB", "BLOCK", "BODY", "BY", "CACHE", "CACHE_INSTANCES", "CANCEL", "CASCADE", "CAST", "CFILE", "CHAINED", "CHANGE", "CHAR", "CHAR_CS", "CHARACTER", "CHECK", "CHECKPOINT", "CHOOSE", "CHUNK", "CLEAR", "CLOB", "CLONE", "CLOSE", "CLOSE_CACHED_OPEN_CURSORS", "CLUSTER", "COALESCE", "COLUMN", "COLUMNS", "COMMENT", "COMMIT", "COMMITTED", "COMPATIBILITY", "COMPILE", "COMPLETE", "COMPOSITE_LIMIT", "COMPRESS", "COMPUTE", "CONNECT", "CONNECT_TIME", "CONSTRAINT", "CONSTRAINTS", "CONTENTS", "CONTINUE", "CONTROLFILE", "CONVERT", "COST", "CPU_PER_CALL", "CPU_PER_SESSION", "CREATE", "CURRENT", "CURRENT_SCHEMA", "CURREN_USER", "CURSOR", "CYCLE", "DANGLING", "DATABASE", "DATAFILE", "DATAFILES", "DATAOBJNO", "DATE", "DBA", "DBHIGH", "DBLOW", "DBMAC", "DEALLOCATE", "DEBUG", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DEGREE", "DELETE", "DEREF", "DESC", "DIRECTORY", "DISABLE", "DISCONNECT", "DISMOUNT", "DISTINCT", "DISTRIBUTED", "DML", "DOUBLE", "DROP", "DUMP", "EACH", "ELSE", "ENABLE", "END", "ENFORCE", "ENTRY", "ESCAPE", "EXCEPT", "EXCEPTIONS", "EXCHANGE", "EXCLUDING", "EXCLUSIVE", "EXECUTE", "EXISTS", "EXPIRE", "EXPLAIN", "EXTENT", "EXTENTS", "EXTERNALLY", "FAILED_LOGIN_ATTEMPTS", "FALSE", "FAST", "FILE", "FIRST_ROWS", "FLAGGER", "FLOAT", "FLOB", "FLUSH", "FOR", "FORCE", "FOREIGN", "FREELIST", "FREELISTS", "FROM", "FULL", "FUNCTION", "GLOBAL", "GLOBALLY", "GLOBAL_NAME", "GRANT", "GROUP", "GROUPS", "HASH", "HASHKEYS", "HAVING", "HEADER", "HEAP", "IDENTIFIED", "IDGENERATORS", "IDLE_TIME", "IF", "IMMEDIATE", "IN", "INCLUDING", "INCREMENT", "INDEX", "INDEXED", "INDEXES", "INDICATOR", "IND_PARTITION", "INITIAL", "INITIALLY", "INITRANS", "INSERT", "INSTANCE", "INSTANCES", "INSTEAD", "INT", "INTEGER", "INTERMEDIATE", "INTERSECT", "INTO", "IS", "ISOLATION", "ISOLATION_LEVEL", "KEEP", "KEY", "KILL", "LABEL", "LAYER", "LESS", "LEVEL", "LIBRARY", "LIKE", "LIMIT", "LINK", "LIST", "LOB", "LOCAL", "LOCK", "LOCKED", "LOG", "LOGFILE", "LOGGING", "LOGICAL_READS_PER_CALL", "LOGICAL_READS_PER_SESSION", "LONG", "MANAGE", "MASTER", "MAX", "MAXARCHLOGS", "MAXDATAFILES", "MAXEXTENTS", "MAXINSTANCES", "MAXLOGFILES", "MAXLOGHISTORY", "MAXLOGMEMBERS", "MAXSIZE", "MAXTRANS", "MAXVALUE", "MIN", "MEMBER", "MINIMUM", "MINEXTENTS", "MINUS", "MINVALUE", "MLSLABEL", "MLS_LABEL_FORMAT", "MODE", "MODIFY", "MOUNT", "MOVE", "MTS_DISPATCHERS", "MULTISET", "NATIONAL", "NCHAR", "NCHAR_CS", "NCLOB", "NEEDED", "NESTED", "NETWORK", "NEW", "NEXT", "NOARCHIVELOG", "NOAUDIT", "NOCACHE", "NOCOMPRESS", "NOCYCLE", "NOFORCE", "NOLOGGING", "NOMAXVALUE", "NOMINVALUE", "NONE", "NOORDER", "NOOVERRIDE", "NOPARALLEL", "NOPARALLEL", "NOREVERSE", "NORMAL", "NOSORT", "NOT", "NOTHING", "NOWAIT", "NULL", "NUMBER", "NUMERIC", "NVARCHAR2", "OBJECT", "OBJNO", "OBJNO_REUSE", "OF", "OFF", "OFFLINE", "OID", "OIDINDEX", "OLD", "ON", "ONLINE", "ONLY", "OPCODE", "OPEN", "OPTIMAL", "OPTIMIZER_GOAL", "OPTION", "OR", "ORDER", "ORGANIZATION", "OSLABEL", "OVERFLOW", "OWN", "PACKAGE", "PARALLEL", "PARTITION", "PASSWORD", "PASSWORD_GRACE_TIME", "PASSWORD_LIFE_TIME", "PASSWORD_LOCK_TIME", "PASSWORD_REUSE_MAX", "PASSWORD_REUSE_TIME", "PASSWORD_VERIFY_FUNCTION", "PCTFREE", "PCTINCREASE", "PCTTHRESHOLD", "PCTUSED", "PCTVERSION", "PERCENT", "PERMANENT", "PLAN", "PLSQL_DEBUG", "POST_TRANSACTION", "PRECISION", "PRESERVE", "PRIMARY", "PRIOR", "PRIVATE", "PRIVATE_SGA", "PRIVILEGE", "PRIVILEGES", "PROCEDURE", "PROFILE", "PUBLIC", "PURGE", "QUEUE", "QUOTA", "RANGE", "RAW", "RBA", "READ", "READUP", "REAL", "REBUILD", "RECOVER", "RECOVERABLE", "RECOVERY", "REF", "REFERENCES", "REFERENCING", "REFRESH", "RENAME", "REPLACE", "RESET", "RESETLOGS", "RESIZE", "RESOURCE", "RESTRICTED", "RETURN", "RETURNING", "REUSE", "REVERSE", "REVOKE", "ROLE", "ROLES", "ROLLBACK", "ROW", "ROWID", "ROWNUM", "ROWS", "RULE", "SAMPLE", "SAVEPOINT", "SB4", "SCAN_INSTANCES", "SCHEMA", "SCN", "SCOPE", "SD_ALL", "SD_INHIBIT", "SD_SHOW", "SEGMENT", "SEG_BLOCK", "SEG_FILE", "SELECT", "SEQUENCE", "SERIALIZABLE", "SESSION", "SESSION_CACHED_CURSORS", "SESSIONS_PER_USER", "SET", "SHARE", "SHARED", "SHARED_POOL", "SHRINK", "SIZE", "SKIP", "SKIP_UNUSABLE_INDEXES", "SMALLINT", "SNAPSHOT", "SOME", "SORT", "SPECIFICATION", "SPLIT", "SQL_TRACE", "STANDBY", "START", "STATEMENT_ID", "STATISTICS", "STOP", "STORAGE", "STORE", "STRUCTURE", "SUCCESSFUL", "SWITCH", "SYS_OP_ENFORCE_NOT_NULL$", "SYS_OP_NTCIMG$", "SYNONYM", "SYSDATE", "SYSDBA", "SYSOPER", "SYSTEM", "TABLE", "TABLES", "TABLESPACE", "TABLESPACE_NO", "TABNO", "TEMPORARY", "THAN", "THE", "THEN", "THREAD", "TIMESTAMP", "TIME", "TO", "TOPLEVEL", "TRACE", "TRACING", "TRANSACTION", "TRANSITIONAL", "TRIGGER", "TRIGGERS", "TRUE", "TRUNCATE", "TX", "TYPE", "UB2", "UBA", "UID", "UNARCHIVED", "UNDO", "UNION", "UNIQUE", "UNLIMITED", "UNLOCK", "UNRECOVERABLE", "UNTIL", "UNUSABLE", "UNUSED", "UPDATABLE", "UPDATE", "USAGE", "USE", "USER", "USING", "VALIDATE", "VALIDATION", "VALUE", "VALUES", "VARCHAR", "VARCHAR2", "VARYING", "VIEW", "WHEN", "WHENEVER", "WHERE", "WITH", "WITHOUT", "WORK", "WRITE", "WRITEDOWN", "WRITEUP", "XID", "YEAR", "ZONE"};
	private static final List<String> reservedWords = Arrays.asList(reservedWordsArray);

	@Override
	public String getDateFormattedForQuery(Date date) {
		if (date == null) {
			return getNull();
		}
//        yyyy-MM-dd hh:mm:ss[.nnnnnnnnn]
		return " TO_TIMESTAMP_TZ('" + javaToStringFormatter.format(date) + "','" + oracleDateFormatStr + "') ";
		//return "'"+strToDateFormat.format(date)+"'";
	}

	@Override
	protected String formatNameForDatabase(final String sqlObjectName) {
		if (sqlObjectName.length() < 30 && !(reservedWords.contains(sqlObjectName.toUpperCase()))) {
			return sqlObjectName.replaceAll("^[_-]", "O").replaceAll("-", "_");
		} else {
			return ("O" + sqlObjectName.hashCode()).replaceAll("^[_-]", "O").replaceAll("-", "_");
		}
	}

	@Override
	public String formatTableAlias(String suggestedTableAlias) {
		return "\"" + suggestedTableAlias.replaceAll("-", "_") + "\"";
	}

	@Override
	public String formatForColumnAlias(final String actualName) {
		String formattedName = actualName.replaceAll("\\.", "__");
		return ("DB" + formattedName.hashCode()).replaceAll("-", "_") + "";
	}

	@Override
	public String beginTableAlias() {
		return " ";
	}

	@Override
	public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
		if (qdt instanceof DBBoolean) {
			return " NUMBER(1)";
		} else if (qdt instanceof DBString) {
			return " VARCHAR(1000) ";
		} else if (qdt instanceof DBDate) {
			return " TIMESTAMP WITH TIME ZONE ";
		} else if (qdt instanceof DBJavaObject) {
			return " BLOB ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBPoint2D) {
			return " VARCHAR(2001) ";
		} else if (qdt instanceof DBLine2D) {
			return " VARCHAR(2002) ";
		} else if (qdt instanceof DBPolygon2D) {
			return " VARCHAR(2003) ";
		} else if (qdt instanceof DBLineSegment2D) {
			return " VARCHAR(2004) ";
		} else if (qdt instanceof DBMultiPoint2D) {
			return " VARCHAR(2005) ";
		} else {
			return qdt.getSQLDatatype();
		}
	}

	@Override
	public boolean supportsArraysNatively() {
		return false;
	}

//    @Override
//    public boolean prefersIndexBasedGroupByClause() {
//        return true;
//    }
	@Override
	public Object endSQLStatement() {
		return "";
	}

	@Override
	public String endInsertLine() {
		return "";
	}

	@Override
	public String endDeleteLine() {
		return "";
	}

	@Override
	public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		return "";
	}

	@Override
	public String getCurrentUserFunctionName() {
		return "USER";
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return "INSTR(" + originalString + "," + stringToFind + ")";
	}

	@Override
	public String getIfNullFunctionName() {
		return "NVL"; //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doStringIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return "DECODE(" + possiblyNullValue + ","
				+ "NULL," + (alternativeIfNull == null ? "NULL" : alternativeIfNull)
				+ ",''," + (alternativeIfNull == null ? "NULL" : alternativeIfNull)
				+ "," + possiblyNullValue + ")";
	}

	@Override
	public String doNumberIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return "DECODE(" + possiblyNullValue
				+ ",NULL," + (alternativeIfNull == null ? "NULL" : alternativeIfNull)
				+ "," + possiblyNullValue + ")";
	}

	@Override
	public String doDateIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return doNumberIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String getStringLengthFunctionName() {
		return "LENGTH";
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTR("
				+ originalString
				+ ", "
				+ start
				+ (length.trim().isEmpty() ? "" : ", " + length)
				+ ") ";
	}

	@Override
	public boolean supportsRadiansFunction() {
		return false;
	}

	@Override
	public boolean supportsDegreesFunction() {
		return false;
	}

	@Override
	public String doModulusTransform(String firstNumber, String secondNumber) {
		return " remainder(" + firstNumber + ", " + secondNumber + ")";
	}

	@Override
	public String doAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfSeconds + ", 'SECOND'))";
	}

	@Override
	public String doAddMinutesTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfSeconds + ", 'MINUTE'))";
	}

	@Override
	public String doAddHoursTransform(String dateValue, String numberOfHours) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfHours + ", 'HOUR'))";
	}

	@Override
	public String doAddDaysTransform(String dateValue, String numberOfDays) {
		return "((" + dateValue + ")+(" + numberOfDays + "))";
	}

	@Override
	public String doAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return doAddDaysTransform(dateValue, "(" + numberOfWeeks + ")*7");
	}

	@Override
	public String doAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "ADD_MONTHS(" + dateValue + ", " + numberOfMonths + ")";
	}

	@Override
	public String doAddYearsTransform(String dateValue, String numberOfYears) {
		return doAddMonthsTransform(dateValue, "(" + numberOfYears + ")*12");
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return getCurrentDateOnlyFunctionName().trim();
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(DAY FROM (CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP))))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "(" + doDayDifferenceTransform(dateValue, otherDateValue) + "/7)";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "MONTHS_BETWEEN(" + otherDateValue + "," + dateValue + ")";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "(MONTHS_BETWEEN(" + otherDateValue + "," + dateValue + ")/12)";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(HOUR FROM (CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP)))"
				+ "+(" + doDayDifferenceTransform(dateValue, otherDateValue) + "*24))";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(MINUTE FROM (CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP)))"
				+ "+(" + doHourDifferenceTransform(dateValue, otherDateValue) + "*60))";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(SECOND FROM (CAST(" + otherDateValue + " AS TIMESTAMP) - CAST(" + dateValue + " AS TIMESTAMP)))"
				+ "+(" + doMinuteDifferenceTransform(dateValue, otherDateValue) + "*60))";
	}

	@Override
	public String doSubsecondTransform(String dateExpression) {
		return doSecondTransform(dateExpression) + "-" + doRoundTransform(doSecondTransform(dateExpression));
	}

	@Override
	public String doInTransform(String column, List<String> values) {
		StringBuilder builder = new StringBuilder();
		builder.append("(")
				.append(column)
				.append(" IN ( ");
		String separator = "";
		for (String val : values) {
			if (val != null && !val.equals(getEmptyString())) {
				builder.append(separator).append(val);
				separator = ", ";
			}
		}
		builder.append("))");
		return builder.toString();
	}

	@Override
	public Object getOrderByDirectionClause(Boolean sortOrder) {
		if (sortOrder == null) {
			return "";
		} else if (sortOrder) {
			return " ASC NULLS FIRST";
		} else {
			return " DESC NULLS LAST";
		}
	}

	@Override
	public String beginWithClause() {
		return " WITH ";
	}

	@Override
	public String doSelectFromRecursiveTable(String recursiveTableAlias, String recursiveAliases) {
		return " SELECT " + recursiveAliases + ", " + getRecursiveQueryDepthColumnName() + " FROM " + recursiveTableAlias + " ORDER BY " + getRecursiveQueryDepthColumnName() + " ASC ";
	}

	/**
	 * Creates a pattern that will exclude system tables during DBRow class
	 * generation i.e. {@link DBTableClassGenerator}.
	 *
	 * <p>
	 * By default this method returns null as system tables are not a problem for
	 * most databases.
	 *
	 * @return
	 */
	@Override
	public String getSystemTableExclusionPattern() {
		return "^[^$]*$"; //"^(.*(?!\\$)\\b)*$";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
//		return " (TO_CHAR("+dateSQL+",'D')+1)";
		return "DECODE(trim(to_char((" + dateSQL + "), 'Day', 'NLS_DATE_LANGUAGE=ENGLISH')), 'Sunday', 1, 'Monday', 2, 'Tuesday', 3, 'Wednesday', 4, 'Thursday', 5, 'Friday', 6, 'Saturday', 7)";
	}

	@Override
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return " TO_NUMBER(" + stringResultContainingANumber + ")";
	}

	@Override
	public boolean supportsCotangentFunction() {
		return false;
	}

	@Override
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		if (columnExpression instanceof BooleanExpression) {
			return ((BooleanExpression) columnExpression).ifThenElse(1, 0);
		} else {
			return super.transformToStorableType(columnExpression);
		}
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "'POINT (" + xValue + " " + yValue + ")'";
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return "(" + line2DSQL + ")";
	}

	@Override
	public String doLine2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String toSQLString) {
		return Line2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}
	
	@Override
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		return "("+Line2DFunctions.INTERSECTSLINE2D + "((" + firstLine+"), ("+secondLine + "))=1)";
	}
	
	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		return Line2DFunctions.INTERSECTNWLINE2D + "((" + firstLine+"), ("+secondLine + "))";
	}


	@Override
	public String doSubstringBeforeTransform(String afterThis, String butBeforeThis) {
		return StringFunctions.SUBSTRINGBEFORE + "(" + afterThis + ", " + butBeforeThis + ")";
	}

	@Override
	public String doSubstringAfterTransform(String fromThis, String afterThis) {
		return StringFunctions.SUBSTRINGAFTER + "(" + fromThis + ", " + afterThis + ")";
	}

	@Override
	public String doDBPolygon2DFormatTransform(Polygon geom) {
		String wktValue = geom.toText();
		return Polygon2DFunctions.CREATE_WKTPOLY2D + "('" + wktValue + "')";
	}

	@Override
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		return Polygon2DFunctions.MINY + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		return Polygon2DFunctions.MAXY + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		return Polygon2DFunctions.MINX + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		return Polygon2DFunctions.MAXX + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		return Polygon2DFunctions.EXTERIORRING + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String polygon2DSQL) {
		return Polygon2DFunctions.AREA + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String polygon2DSQL) {
		return Polygon2DFunctions.BOUNDINGBOX + "(" + polygon2DSQL + ")";
	}

	@Override
	public String doPolygon2DGetDimensionTransform(String toSQLString) {
		return Polygon2DFunctions.DIMENSION + "(" + toSQLString + ")";
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.WITHIN + "(" + firstGeometry + ", " + secondGeometry + ")";	
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.TOUCHES + "(" + firstGeometry + ", " + secondGeometry + ")";	
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.OVERLAPS + "(" + firstGeometry + ", " + secondGeometry + ")";	
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.DISJOINT + "(" + firstGeometry + ", " + secondGeometry + ")";	
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.CONTAINS + "(" + firstGeometry + ", " + secondGeometry + ")";	
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTS + "(" + firstGeometry + ", " + secondGeometry + ")";	
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return Polygon2DFunctions.INTERSECTION + "(" + firstGeometry + ", " + secondGeometry + ")";	
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return "("+Polygon2DFunctions.EQUALS + "(" + firstGeometry + ", " + secondGeometry + ")=1)";
	}
	
	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String firstSQL, String secondSQL) {
		return "("+LineSegment2DFunctions.INTERSECTS_LSEG2D+"(("+firstSQL+"), ("+secondSQL+"))=1)";
	}

	/**
	 * Generate the SQL required to find the largest X value in the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetMaxXTransform(String lineSegment) {
		return LineSegment2DFunctions.MAXX+"("+lineSegment+")";
	}

	/**
	 * Generate the SQL required to find the smallest X value in the line segment SQL expression.
	 * 
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetMinXTransform(String lineSegment) {
		return LineSegment2DFunctions.MINX+"("+lineSegment+")";
	}

	/**
	 * Generate the SQL required to find the largest Y value in the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetMaxYTransform(String lineSegment) {
		return LineSegment2DFunctions.MAXY+"("+lineSegment+")";
	}

	/**
	 * Generate the SQL required to find the smallest Y value in the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetMinYTransform(String lineSegment) {
		return LineSegment2DFunctions.MINY+"("+lineSegment+")";
	}

	/**
	 * Generate the SQL required to the rectangular boundary that fully encloses the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String lineSegment) {
		return LineSegment2DFunctions.BOUNDINGBOX+"("+lineSegment+")";
	}

	/**
	 * Generate the SQL required to find the dimension of the line segment SQL expression.
	 *
	 * @param lineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DDimensionTransform(String lineSegment) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 * Generate the SQL required to find whether the 2 line segment SQL expressions are NOT equal.
	 *
	 * @param firstLineSegment
	 * @param secondLineSegment
	 * @return SQL
	 */
	@Override
	public String doLineSegment2DNotEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return " NOT "+LineSegment2DFunctions.EQUALS+"(("+firstLineSegment+"), ("+secondLineSegment+"))";
	}

	/**
	 * Generate the SQL required to find whether the 2 line segment SQL expressions are equal.
	 *
	 * @param firstLineSegment
	 * @param secondLineSegment
	 * @return
	 */
	@Override
	public String doLineSegment2DEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return "("+LineSegment2DFunctions.EQUALS+"(("+firstLineSegment+"), ("+secondLineSegment+"))=1)";
	}

	/**
	 * Generate the SQL required to convert the line segment SQL expression into the WKT string format.
	 *
	 * @param lineSegment
	 * @return
	 */
	@Override
	public String doLineSegment2DAsTextTransform(String lineSegment) {
		return lineSegment;
	}

	/**
	 * Generate the SQL required to find the intersection point of the 2 line segment SQL expressions.
	 *
	 * @param firstLineSegment
	 * @param secondLineSegment
	 * @return an SQL expression that will evaluate to the intersection point of the 2 line segments or NULL.
	 */
	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return LineSegment2DFunctions.INTERSECTPT_LSEG2D+"(("+firstLineSegment+"), ("+secondLineSegment+"))";
	}

	/**
	 * Provide the SQL that correctly represents this MultiPoint2D value in this database.
	 *
	 * @param points
	 * @return SQL
	 */
	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		String wktValue = points.toText();
		return "'" + wktValue + "'";
	}

	/**
	 * Convert the database's string representation of a MultiPoint2D value into a MultiPoint..
	 *
	 * @param pointsAsString 
	 * @return MultiPoint
	 * @throws com.vividsolutions.jts.io.ParseException
	 */
	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		MultiPoint mpoint = null;
		WKTReader wktReader = new WKTReader();
		Geometry geometry = wktReader.read(pointsAsString);
		if (geometry instanceof MultiPoint) {
			mpoint = (MultiPoint) geometry;
		} else if (geometry instanceof Point) {
			mpoint = (new GeometryFactory().createMultiPoint(new Point[]{((Point)geometry)}));
		} else {
			throw new IncorrectGeometryReturnedForDatatype(geometry, geometry);
		}
		return mpoint;
	}

	/**
	 * Provide the SQL to compare 2 MultiPoint2Ds
	 *
	 * @param first
	 * @param second
	 * @return SQL
	 */
	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return "("+MultiPoint2DFunctions.EQUALS+"("+first+", "+second+")=1)";
	}

	/**
	 * Provide the SQL to get point at the supplied index within the MultiPoint2D
	 *
	 * @param first
	 * @param index 
	 * @return SQL
	 */
	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return ""+MultiPoint2DFunctions.GETFROMINDEX+"("+first+", "+index+")";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String first) {
		return ""+MultiPoint2DFunctions.POINTCOUNT+"("+first+")";
	}

	@Override
	public String doMultiPoint2DDimensionTransform(String first) {
		return ""+MultiPoint2DFunctions.DIMENSION+"("+first+")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String first) {
		return ""+MultiPoint2DFunctions.BOUNDINGBOX+"("+first+")";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String first) {
		return ""+MultiPoint2DFunctions.ASTEXT+"("+first+")";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String first) {
		return ""+MultiPoint2DFunctions.ASLINE2D+"("+first+")";
	}

	@Override
	public String doMultiPoint2DToPolygon2DTransform(String first) {
		return ""+MultiPoint2DFunctions.ASPOLY2D+"("+first+")";
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String first) {
		return ""+MultiPoint2DFunctions.MINY+"("+first+")";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String first) {
		return ""+MultiPoint2DFunctions.MINX+"("+first+")";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String first) {
		return ""+MultiPoint2DFunctions.MAXY+"("+first+")";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String first) {
		return ""+MultiPoint2DFunctions.MAXX+"("+first+")";
	}
	
}
