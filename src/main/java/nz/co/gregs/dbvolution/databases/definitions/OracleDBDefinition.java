/*
 * Copyright 2015 gregorygraham.
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBBooleanArray;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBDuration;
import nz.co.gregs.dbvolution.datatypes.DBInstant;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBLargeText;
import nz.co.gregs.dbvolution.datatypes.DBLocalDate;
import nz.co.gregs.dbvolution.datatypes.DBLocalDateTime;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.CaseExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.InstantExpression;
import nz.co.gregs.dbvolution.expressions.LocalDateExpression;
import nz.co.gregs.dbvolution.expressions.LocalDateTimeExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.internal.oracle.StringFunctions;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.query.QueryState;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.utility.TemporalStringParser;

/**
 * Defines the features of the Oracle database that differ from the standard
 * database.
 *
 * <p>
 * Provides the base definitions used by all variants of the Oracle database
 * DBDefinition.
 *
 * @author Gregory Graham
 */
public class OracleDBDefinition extends DBDefinition {

	public static final long serialVersionUID = 1L;

	private final static String DATE_FORMAT_STRING_WITH_TIMEZONE = "yyyy-M-d HH:mm:ss.SSS Z";
	private final static String ORACLE_DATE_FORMAT_STRING_WITH_TIMEZONE = "YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM";
	private final SimpleDateFormat JAVA_TO_STRING_FORMATTER_WITH_TIMEZONE = new SimpleDateFormat(DATE_FORMAT_STRING_WITH_TIMEZONE);
	private static final String[] RESERVED_WORDS_ARRAY = new String[]{"ACCESS", "ACCOUNT", "ACTIVATE", "ADD", "ADMIN", "ADVISE", "AFTER", "ALL", "ALL_ROWS", "ALLOCATE", "ALTER", "ANALYZE", "AND", "ANY", "ARCHIVE", "ARCHIVELOG", "ARRAY", "AS", "ASC", "AT", "AUDIT", "AUTHENTICATED", "AUTHORIZATION", "AUTOEXTEND", "AUTOMATIC", "BACKUP", "BECOME", "BEFORE", "BEGIN", "BETWEEN", "BFILE", "BITMAP", "BLOB", "BLOCK", "BODY", "BY", "CACHE", "CACHE_INSTANCES", "CANCEL", "CASCADE", "CAST", "CFILE", "CHAINED", "CHANGE", "CHAR", "CHAR_CS", "CHARACTER", "CHECK", "CHECKPOINT", "CHOOSE", "CHUNK", "CLEAR", "CLOB", "CLONE", "CLOSE", "CLOSE_CACHED_OPEN_CURSORS", "CLUSTER", "COALESCE", "COLUMN", "COLUMNS", "COMMENT", "COMMIT", "COMMITTED", "COMPATIBILITY", "COMPILE", "COMPLETE", "COMPOSITE_LIMIT", "COMPRESS", "COMPUTE", "CONNECT", "CONNECT_TIME", "CONSTRAINT", "CONSTRAINTS", "CONTENTS", "CONTINUE", "CONTROLFILE", "CONVERT", "COST", "CPU_PER_CALL", "CPU_PER_SESSION", "CREATE", "CURRENT", "CURRENT_SCHEMA", "CURREN_USER", "CURSOR", "CYCLE", "DANGLING", "DATABASE", "DATAFILE", "DATAFILES", "DATAOBJNO", "DATE", "DBA", "DBHIGH", "DBLOW", "DBMAC", "DEALLOCATE", "DEBUG", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DEGREE", "DELETE", "DEREF", "DESC", "DIRECTORY", "DISABLE", "DISCONNECT", "DISMOUNT", "DISTINCT", "DISTRIBUTED", "DML", "DOUBLE", "DROP", "DUMP", "EACH", "ELSE", "ENABLE", "END", "ENFORCE", "ENTRY", "ESCAPE", "EXCEPT", "EXCEPTIONS", "EXCHANGE", "EXCLUDING", "EXCLUSIVE", "EXECUTE", "EXISTS", "EXPIRE", "EXPLAIN", "EXTENT", "EXTENTS", "EXTERNALLY", "FAILED_LOGIN_ATTEMPTS", "FALSE", "FAST", "FILE", "FIRST_ROWS", "FLAGGER", "FLOAT", "FLOB", "FLUSH", "FOR", "FORCE", "FOREIGN", "FREELIST", "FREELISTS", "FROM", "FULL", "FUNCTION", "GLOBAL", "GLOBALLY", "GLOBAL_NAME", "GRANT", "GROUP", "GROUPS", "HASH", "HASHKEYS", "HAVING", "HEADER", "HEAP", "IDENTIFIED", "IDGENERATORS", "IDLE_TIME", "IF", "IMMEDIATE", "IN", "INCLUDING", "INCREMENT", "INDEX", "INDEXED", "INDEXES", "INDICATOR", "IND_PARTITION", "INITIAL", "INITIALLY", "INITRANS", "INSERT", "INSTANCE", "INSTANCES", "INSTEAD", "INT", "INTEGER", "INTERMEDIATE", "INTERSECT", "INTO", "IS", "ISOLATION", "ISOLATION_LEVEL", "KEEP", "KEY", "KILL", "LABEL", "LAYER", "LESS", "LEVEL", "LIBRARY", "LIKE", "LIMIT", "LINK", "LIST", "LOB", "LOCAL", "LOCK", "LOCKED", "LOG", "LOGFILE", "LOGGING", "LOGICAL_READS_PER_CALL", "LOGICAL_READS_PER_SESSION", "LONG", "MANAGE", "MASTER", "MAX", "MAXARCHLOGS", "MAXDATAFILES", "MAXEXTENTS", "MAXINSTANCES", "MAXLOGFILES", "MAXLOGHISTORY", "MAXLOGMEMBERS", "MAXSIZE", "MAXTRANS", "MAXVALUE", "MIN", "MEMBER", "MINIMUM", "MINEXTENTS", "MINUS", "MINVALUE", "MLSLABEL", "MLS_LABEL_FORMAT", "MODE", "MODIFY", "MOUNT", "MOVE", "MTS_DISPATCHERS", "MULTISET", "NATIONAL", "NCHAR", "NCHAR_CS", "NCLOB", "NEEDED", "NESTED", "NETWORK", "NEW", "NEXT", "NOARCHIVELOG", "NOAUDIT", "NOCACHE", "NOCOMPRESS", "NOCYCLE", "NOFORCE", "NOLOGGING", "NOMAXVALUE", "NOMINVALUE", "NONE", "NOORDER", "NOOVERRIDE", "NOPARALLEL", "NOPARALLEL", "NOREVERSE", "NORMAL", "NOSORT", "NOT", "NOTHING", "NOWAIT", "NULL", "NUMBER", "NUMERIC", "NVARCHAR2", "OBJECT", "OBJNO", "OBJNO_REUSE", "OF", "OFF", "OFFLINE", "OID", "OIDINDEX", "OLD", "ON", "ONLINE", "ONLY", "OPCODE", "OPEN", "OPTIMAL", "OPTIMIZER_GOAL", "OPTION", "OR", "ORDER", "ORGANIZATION", "OSLABEL", "OVERFLOW", "OWN", "PACKAGE", "PARALLEL", "PARTITION", "PASSWORD", "PASSWORD_GRACE_TIME", "PASSWORD_LIFE_TIME", "PASSWORD_LOCK_TIME", "PASSWORD_REUSE_MAX", "PASSWORD_REUSE_TIME", "PASSWORD_VERIFY_FUNCTION", "PCTFREE", "PCTINCREASE", "PCTTHRESHOLD", "PCTUSED", "PCTVERSION", "PERCENT", "PERMANENT", "PLAN", "PLSQL_DEBUG", "POST_TRANSACTION", "PRECISION", "PRESERVE", "PRIMARY", "PRIOR", "PRIVATE", "PRIVATE_SGA", "PRIVILEGE", "PRIVILEGES", "PROCEDURE", "PROFILE", "PUBLIC", "PURGE", "QUEUE", "QUOTA", "RANGE", "RAW", "RBA", "READ", "READUP", "REAL", "REBUILD", "RECOVER", "RECOVERABLE", "RECOVERY", "REF", "REFERENCES", "REFERENCING", "REFRESH", "RENAME", "REPLACE", "RESET", "RESETLOGS", "RESIZE", "RESOURCE", "RESTRICTED", "RETURN", "RETURNING", "REUSE", "REVERSE", "REVOKE", "ROLE", "ROLES", "ROLLBACK", "ROW", "ROWID", "ROWNUM", "ROWS", "RULE", "SAMPLE", "SAVEPOINT", "SB4", "SCAN_INSTANCES", "SCHEMA", "SCN", "SCOPE", "SD_ALL", "SD_INHIBIT", "SD_SHOW", "SEGMENT", "SEG_BLOCK", "SEG_FILE", "SELECT", "SEQUENCE", "SERIALIZABLE", "SESSION", "SESSION_CACHED_CURSORS", "SESSIONS_PER_USER", "SET", "SHARE", "SHARED", "SHARED_POOL", "SHRINK", "SIZE", "SKIP", "SKIP_UNUSABLE_INDEXES", "SMALLINT", "SNAPSHOT", "SOME", "SORT", "SPECIFICATION", "SPLIT", "SQL_TRACE", "STANDBY", "START", "STATEMENT_ID", "STATISTICS", "STOP", "STORAGE", "STORE", "STRUCTURE", "SUCCESSFUL", "SWITCH", "SYS_OP_ENFORCE_NOT_NULL$", "SYS_OP_NTCIMG$", "SYNONYM", "SYSDATE", "SYSDBA", "SYSOPER", "SYSTEM", "TABLE", "TABLES", "TABLESPACE", "TABLESPACE_NO", "TABNO", "TEMPORARY", "THAN", "THE", "THEN", "THREAD", "TIMESTAMP", "TIME", "TO", "TOPLEVEL", "TRACE", "TRACING", "TRANSACTION", "TRANSITIONAL", "TRIGGER", "TRIGGERS", "TRUE", "TRUNCATE", "TX", "TYPE", "UB2", "UBA", "UID", "UNARCHIVED", "UNDO", "UNION", "UNIQUE", "UNLIMITED", "UNLOCK", "UNRECOVERABLE", "UNTIL", "UNUSABLE", "UNUSED", "UPDATABLE", "UPDATE", "USAGE", "USE", "USER", "USING", "VALIDATE", "VALIDATION", "VALUE", "VALUES", "VARCHAR", "VARCHAR2", "VARYING", "VIEW", "WHEN", "WHENEVER", "WHERE", "WITH", "WITHOUT", "WORK", "WRITE", "WRITEDOWN", "WRITEUP", "XID", "YEAR", "ZONE"};
	private static final List<String> RESERVED_WORDS_LIST = Arrays.asList(RESERVED_WORDS_ARRAY);

	@Override
	public String getDateFormattedForQuery(Date date) {
		if (date == null) {
			return getNull();
		}
		return "/*getDateFormattedForQuery*/ TO_TIMESTAMP_TZ('" + JAVA_TO_STRING_FORMATTER_WITH_TIMEZONE.format(date) + "','" + ORACLE_DATE_FORMAT_STRING_WITH_TIMEZONE + "') ";
	}

	@Override
	public String getDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return "/*getDatePartsFormattedForQuery*/TO_TIMESTAMP_TZ("
				+ years
				+ "||'-'||" + months
				+ "||'-'||" + days
				+ "||' '||" + hours
				+ "||':'||" + minutes
				+ "||':'||to_char(" + seconds + "+" + subsecond + ", '90D099999')"
				+ "||' '||'" + timeZoneSign + "'"
				+ "||" + timeZoneHourOffset
				+ "||':'||" + timeZoneMinuteOffSet
				+ ", '" + "YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM" + "')";
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return true;
	}

	DateTimeFormatter[] PARSE_ZONEDDATETIME_FORMATS = new DateTimeFormatter[]{
		DateTimeFormatter.ofPattern("y-M-d H:m:s Z"),
		DateTimeFormatter.ofPattern("y-M-d H:m:s.S Z"),
		DateTimeFormatter.ofPattern("y-M-d H:m:s.SSSSSS Z"),
		DateTimeFormatter.ofPattern("y-M-d H:m:s.n Z"),
		DateTimeFormatter.ofPattern("y-M-d H:m:s.S X"),
		DateTimeFormatter.ofPattern("y-M-d H:m:s.SSSSSS X"),
		DateTimeFormatter.ofPattern("y-M-d H:m:s.S XXX"),// this works for LocalDate
		DateTimeFormatter.ofPattern("y-M-d H:m:s.SSSSSS XXX"),// this works for Instant
		DateTimeFormatter.ofPattern("y-M-d H:m:s.n XXX"),
		DateTimeFormatter.ofPattern("y-M-d H:m:s.S VV"),
		DateTimeFormatter.ofPattern("y-M-d H:m:s.SSSSSS VV"),// works for named offsets
		DateTimeFormatter.ofPattern("y-M-d H:m:s.nnnnnn XXX")
	};

	public ZonedDateTime parseZonedDateTimeFromGetString(String inputFromResultSet) throws DateTimeParseException {
		return TemporalStringParser.toZonedDateTime(inputFromResultSet);
	}

	@Override
	public LocalDateTime parseLocalDateTimeFromGetString(String inputFromResultSet) throws ParseException {
		return parseZonedDateTimeFromGetString(inputFromResultSet).toLocalDateTime();
	}

	@Override
	public LocalDate parseLocalDateFromGetString(String getStringDate) throws ParseException {
		return parseZonedDateTimeFromGetString(getStringDate).toLocalDate();
	}

	@Override
	public Instant parseInstantFromGetString(String inputFromResultSet) throws ParseException {
		return parseZonedDateTimeFromGetString(inputFromResultSet).toInstant();
	}

	@Override
	protected String formatNameForDatabase(final String sqlObjectName) {
		if (sqlObjectName.length() < 30 && !(RESERVED_WORDS_LIST.contains(sqlObjectName.toUpperCase()))) {
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
	public String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBBoolean) {
			return " NUMBER(1)";
		} else if (qdt instanceof DBNumber) {
			return " NUMBER(38,16) ";
		} else if (qdt instanceof DBInteger) {
			return " NUMBER(38,0) ";
		} else if (qdt instanceof DBString) {
			return " VARCHAR(1000) ";
		} else if (qdt instanceof DBDate) {
			return " TIMESTAMP WITH TIME ZONE ";
		} else if (qdt instanceof DBLocalDate) {
			return " TIMESTAMP WITH TIME ZONE ";
		} else if (qdt instanceof DBLocalDateTime) {
			return " TIMESTAMP WITH TIME ZONE ";
		} else if (qdt instanceof DBInstant) {
			return " TIMESTAMP WITH TIME ZONE ";
		} else if (qdt instanceof DBJavaObject) {
			return " BLOB ";
		} else if (qdt instanceof DBBooleanArray) {
			return " VARCHAR(64) ";
		} else if (qdt instanceof DBDuration) {
			return " INTERVAL DAY(9) TO SECOND(9) ";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
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
	public String endSQLStatement() {
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
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
		return "";
	}

	@Override
	public String doWrapQueryForPaging(String sqlQuery, QueryOptions options) {
		if (options.getRowLimit() > -1) {
			final int firstRowOfNextPage = (options.getPageIndex() + 1) * options.getRowLimit() + 1;
			final int firstRowOfPage = options.getPageIndex() * options.getRowLimit() + 1;
			return "select *\n"
					+ "  from ( select /*+ FIRST_ROWS(n) */\n"
					+ "  a.*, ROWNUM rnum\n"
					+ "      from ( " + sqlQuery + " ) a\n"
					+ "      where ROWNUM <" + firstRowOfNextPage + "\n"
					+ "      )\n"
					+ "where rnum  >= " + firstRowOfPage + "";
		} else {
			return super.doWrapQueryForPaging(sqlQuery, options);
		}
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
		return "NVL(" + possiblyNullValue + ", " + alternativeIfNull + ")";
	}

	@Override
	public String doNumberIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return "NVL(" + possiblyNullValue + ", " + alternativeIfNull + ")";
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
	public String doSubstringTransform(String originalString, String start, String length
	) {
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
	public String doDateAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfSeconds + ", 'SECOND'))";
	}

	@Override
	public String doDateAddMinutesTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfSeconds + ", 'MINUTE'))";
	}

	@Override
	public String doDateAddHoursTransform(String dateValue, String numberOfHours) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfHours + ", 'HOUR'))";
	}

	@Override
	public String doDateAddDaysTransform(String dateValue, String numberOfDays) {
		return "((" + dateValue + ")+(INTERVAL '1' DAY*(" + numberOfDays + ")))";
	}

	@Override
	public String doDateAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return doDateAddDaysTransform(dateValue, "(" + numberOfWeeks + ")*7");
	}

	@Override
	public String doDateAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "ADD_MONTHS(" + dateValue + ", " + numberOfMonths + ")";
	}

	@Override
	public String doDateAddYearsTransform(String dateValue, String numberOfYears) {
		return doDateAddMonthsTransform(dateValue, "(" + numberOfYears + ")*12");
	}

	@Override
	public String doInstantAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfSeconds + ", 'SECOND'))";
	}

	@Override
	public String doInstantAddMinutesTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfSeconds + ", 'MINUTE'))";
	}

	@Override
	public String doInstantAddHoursTransform(String dateValue, String numberOfHours) {
		return "(" + dateValue + " + numtodsinterval( " + numberOfHours + ", 'HOUR'))";
	}

	@Override
	public String doInstantAddDaysTransform(String dateValue, String numberOfDays) {
		return "((" + dateValue + ")+(INTERVAL '1' DAY*(" + numberOfDays + ")))";
	}

	@Override
	public String doInstantAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return doDateAddDaysTransform(dateValue, "(" + numberOfWeeks + ")*7");
	}

	@Override
	public String doInstantAddMonthsTransform(String dateValue, String numberOfMonths) {
		return "ADD_MONTHS(" + dateValue + ", " + numberOfMonths + ")";
	}

	@Override
	public String doInstantAddYearsTransform(String dateValue, String numberOfYears) {
		return doDateAddMonthsTransform(dateValue, "(" + numberOfYears + ")*12");
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return getCurrentDateOnlyFunctionName().trim();
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(DAY FROM (CAST(" + otherDateValue + " AS TIMESTAMP WITH TIME ZONE) - CAST(" + dateValue + " AS TIMESTAMP WITH TIME ZONE))))";
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
		return "(EXTRACT(HOUR FROM (CAST(" + otherDateValue + " AS TIMESTAMP WITH TIME ZONE) - CAST(" + dateValue + " AS TIMESTAMP WITH TIME ZONE)))"
				+ "+(" + doDayDifferenceTransform(dateValue, otherDateValue) + "*24))";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(MINUTE FROM (CAST(" + otherDateValue + " AS TIMESTAMP WITH TIME ZONE) - CAST(" + dateValue + " AS TIMESTAMP WITH TIME ZONE)))"
				+ "+(" + doHourDifferenceTransform(dateValue, otherDateValue) + "*60))";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(SECOND FROM (CAST(" + otherDateValue + " AS TIMESTAMP WITH TIME ZONE) - CAST(" + dateValue + " AS TIMESTAMP WITH TIME ZONE)))"
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
	public String getOrderByDescending() {
		return " DESC ";
	}

	@Override
	public String getOrderByAscending() {
		return " ASC ";
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
	 * @return a regexp pattern
	 */
	@Override
	public String getSystemTableExclusionPattern() {
		return "^[^$]*$"; //"^(.*(?!\\$)\\b)*$";
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return "DECODE(trim(to_char((" + dateSQL + "), 'Day', 'NLS_DATE_LANGUAGE=ENGLISH')), 'Sunday', 1, 'Monday', 2, 'Tuesday', 3, 'Wednesday', 4, 'Thursday', 5, 'Friday', 6, 'Saturday', 7)";
	}

	@Override
	public String doInstantDayOfWeekTransform(String dateSQL) {
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

	/**
	 * Transform a Java Boolean into the equivalent in an SQL snippet.
	 *
	 * @param boolValue	boolValue
	 * @return an SQL snippet
	 */
	@Override
	public String doBooleanValueTransform(Boolean boolValue) {
		if (boolValue == null) {
			return getNull();
		} else if (boolValue) {
			return getTrueValue();
		} else {
			return getFalseValue();
		}
	}

	/**
	 * The value used for TRUE boolean values.
	 *
	 * <p>
	 * The default method returns " TRUE ".
	 *
	 * @return " TRUE "
	 */
	@Override
	public String getTrueValue() {
		return " 1 ";
	}

	/**
	 * The value used for FALSE boolean values.
	 *
	 * <p>
	 * The default method returns " FALSE ".
	 *
	 * @return " FALSE "
	 */
	@Override
	public String getFalseValue() {
		return " 0 ";
	}

	/**
	 * An SQL snippet that always evaluates to FALSE for this database.
	 *
	 * @return " 1=0 " or equivalent
	 */
	@Override
	public String getFalseOperation() {
		return " (1=0) ";
	}

	/**
	 * An SQL snippet that always evaluates to TRUE for this database.
	 *
	 * @return " 1=1 " or equivalent
	 */
	@Override
	public String getTrueOperation() {
		return " (1=1) ";
	}

	@Override
	public boolean supportsComparingBooleanResults() {
		return false;
	}

	@Override
	public String doColumnTransformForSelect(QueryableDatatype<?> qdt, String selectableName) {
		return super.doColumnTransformForSelect(qdt, selectableName);
	}

	@Override
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		return super.transformToStorableType(columnExpression);
	}

	@Override
	public DBExpression transformToSortableType(DBExpression columnExpression) {
		if (columnExpression instanceof BooleanExpression) {
			return ((BooleanExpression) columnExpression).ifTrueFalseNull(1, 0, null);
		}
		return super.transformToSortableType(columnExpression);
	}

	@Override
	public DBExpression transformToGroupableType(DBExpression expression) {
		if (expression instanceof BooleanExpression) {
			return ((BooleanExpression) expression).ifTrueFalseNull(1, 0, null);
		} else {
			return super.transformToGroupableType(expression);
		}
	}

	@Override
	public DBExpression transformToSelectableType(DBExpression expression) {
		if (expression instanceof CaseExpression) {
			return super.transformToSelectableType(expression);
		} else if (expression instanceof BooleanExpression) {
			BooleanExpression boolExpr = (BooleanExpression) expression;
			if (boolExpr.isWindowingFunction()) {
				return super.transformToSelectableType(expression);
			} else if (!boolExpr.isBooleanStatement()) {
				return super.transformToSelectableType(expression);
			} else {
				return boolExpr.ifTrueFalseNull(1, 0, null);
			}
		} else if (expression instanceof LocalDateExpression) {
			return transformToStandardDateFormatForRetrieval(expression);
		} else if (expression instanceof LocalDateTimeExpression) {
			return transformToStandardDateFormatForRetrieval(expression);
		} else if (expression instanceof InstantExpression) {
			return transformToStandardDateFormatForRetrieval(expression);
		} else {
			return super.transformToSelectableType(expression);
		}
	}

	@Override
	public DBExpression transformToWhenableType(BooleanExpression test) {
		if (test.isBooleanStatement()) {
			return test;
		} else {
			return new BooleanExpression(test) {
				@Override
				public String toSQLString(DBDefinition db) {
					return "(" + super.toSQLString(db) + "=1)";
				}
			};
		}
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
	public String doEndOfMonthTransform(String dateSQL) {
		return "LAST_DAY(" + dateSQL + ")";
	}

	@Override
	public String doInstantEndOfMonthTransform(String dateSQL) {
		return "FROM_TZ(CAST(LAST_DAY(" + dateSQL + ") AS TIMESTAMP), 'UTC')";
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectWriter(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.CHARSTREAM;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BLOB;
		} else {
			return super.preferredLargeObjectWriter(lob);
		}
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectReader(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.STRING;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BLOB;
		} else {
			return super.preferredLargeObjectReader(lob);
		}
	}

	/**
	 * Returns the required code to generate a random number.
	 *
	 * <p>
	 * For each call of this method a new random number is generated.
	 *
	 * <p>
	 * This method DOES NOT use the SQLServer built-in function as it does not
	 * produce a different result for different rows in a single query.
	 *
	 * @return random number generating code
	 */
	@Override
	public String doRandomNumberTransform() {
		return " DBMS_RANDOM.VALUE ";
	}

	@Override
	public String doLogBase10NumberTransform(String sql) {
		return "log(10, (" + sql + "))";
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return "(cast(to_char(" + dateExpression + ", 'DD') as number))";
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return "(cast(to_char(" + dateExpression + ", 'HH24') as number))";
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return "(cast(to_char(" + dateExpression + ", 'MM') as number))";
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return "(cast(to_char(" + dateExpression + ", 'YYYY') as number))";
	}

	@Override
	public String doFindNumberInStringTransform(String toSQLString) {
		return "(case when regexp_replace(" + toSQLString + ",'.*?([-]?[0-9]+(\\.[0-9]+)?).*$', '\\1') = " + toSQLString + " then " + getNull() + " else regexp_replace(" + toSQLString + ",'.*?([-]?[0-9]+(\\.[0-9]+)?).*$', '\\1') end)";
	}

	@Override
	public String doFindIntegerInStringTransform(String toSQLString) {
		return "(case when regexp_replace(" + toSQLString + ",'.*?([-]?[0-9]+).*$', '\\1') = " + toSQLString + " then " + getNull() + " else regexp_replace(" + toSQLString + ",'.*?([-]?[0-9]+).*$', '\\1') end)";
	}

	/**
	 * Oracle does not differentiate between NULL and an empty string.
	 *
	 * @return FALSE.
	 */
	@Override
	public synchronized Boolean supportsDifferenceBetweenNullAndEmptyStringNatively() {
		return false;
	}

	@Override
	public boolean supportsTableCheckingViaMetaData() {
		return false;
	}

	@Override
	public String getTableExistsSQL(DBRow table) {
		return "SELECT COUNT(*) FROM " + this.formatTableName(table);
	}

	/**
	 * Provides the start of the DROP TABLE expression for this database.
	 *
	 * @return "DROP TABLE " or equivalent for the database.
	 */
	@Override
	public String getDropTableStart() {
		return "DROP TABLE ";
	}

	@Override
	public String getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return " /*+ FIRST_ROWS(" + options.getRowLimit() + ") */ ";
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return "";
	}

	@Override
	public boolean prefersTriggerBasedIdentities() {
		return true;
	}

	@Override
	public List<String> getTriggerBasedIdentitySQL(DBDatabase DB, String table, String column) {

		List<String> result = new ArrayList<>();
		String sequenceName = getPrimaryKeySequenceName(table, column);
		final String primaryKeyTriggerName = getPrimaryKeyTriggerName(table, column);
		result.add("DROP TRIGGER " + primaryKeyTriggerName + "");
		result.add("DROP SEQUENCE " + sequenceName + "");
		result.add("CREATE SEQUENCE " + sequenceName);
		result.add("CREATE OR REPLACE TRIGGER " + DB.getUsername() + "." + primaryKeyTriggerName + " \n"
				+ "    BEFORE INSERT ON " + DB.getUsername() + "." + table + " \n"
				+ "    FOR EACH ROW\n"
				+ "    WHEN (new." + column + " IS NULL)\n"
				+ "    BEGIN\n"
				+ "      SELECT " + sequenceName + ".NEXTVAL\n"
				+ "      INTO   :new." + column + "\n"
				+ "      FROM   dual;\n"
				+ "    END;\n");

		return result;
	}

	@Override
	public List<String> dropTriggerBasedIdentitySQL(DBDatabase DB, String table, String column) {
		List<String> result = new ArrayList<>();
		result.add("DROP TRIGGER " + getPrimaryKeyTriggerName(table, column) + "");
		result.add("DROP SEQUENCE " + getPrimaryKeySequenceName(table, column) + "");
		return result;
	}

	@Override
	public String getAlterTableAddColumnSQL(DBRow existingTable, PropertyWrapper<?, ?, ?> columnPropertyWrapper) {
		return "ALTER TABLE " + formatTableName(existingTable) + " ADD " + getAddColumnColumnSQL(columnPropertyWrapper) + endSQLStatement();
	}

	@Override
	public String doNumberToIntegerTransform(String sql) {
		return "CAST(" + sql + " AS NUMBER(38,0))";
	}

	@Override
	public String doCurrentUTCDateTimeTransform() {
		return "(SYSTIMESTAMP at time zone 'UTC')";
	}

	/**
	 * Creates the CURRENTTIME function for this database.
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	@Override
	public String doCurrentUTCTimeTransform() {
		return "(SYSTIMESTAMP at time zone 'UTC')";
	}

	@Override
	public String doInstantSubsecondTransform(String dateExpression) {
		return doInstantSecondTransform(dateExpression) + "-" + doRoundTransform(doInstantSecondTransform(dateExpression));
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String referencedTable) {
		return "LISTAGG(" + accumulateColumn + ", " + doStringLiteralWrapping(separator) + ") WITHIN GROUP( ORDER BY 1 )";
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String orderByColumnName, String referencedTable) {
		return "LISTAGG(" + accumulateColumn + ", " + doStringLiteralWrapping(separator) + ")" + " WITHIN GROUP( ORDER BY " + orderByColumnName + ")";
	}

	private DBExpression transformToStandardDateFormatForRetrieval(DBExpression expression) {
		return new StringExpression((AnyResult<?>) expression) {
			@Override
			public String toSQLString(DBDefinition db) {
				return "to_char( CAST(" + expression.toSQLString(db) + " AS TIMESTAMP WITH TIME ZONE), 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')";
			}
		};
	}

	@Override
	public boolean requiresOnClauseForAllJoins() {
		return true;
	}

	@Override
	public int getParseDurationPartOffset() {
		return 0;
	}

	@Override
	public String formatPrimaryKeyForRetrievingGeneratedKeys(String primaryKeyColumnName) {
		return formatColumnName(primaryKeyColumnName);
	}

	@Override
	public String getFromDualEquivalent() {
		return " FROM DUAL ";
	}

	@Override
	public boolean supportsDateRepeatDatatypeFunctions() {
		return false;
	}

	@Override
	public String getTableStructureQuery(DBRow table, DBTable<?> dbTable) {
		final String sqlForQuery = " select *\n"
				+ "  from ( select /*+ FIRST_ROWS(n) */\n"
				+ "  a.*, ROWNUM rnum\n"
				+ "      from (  SELECT  /*+ FIRST_ROWS(1) */ *"
				+ " FROM  " + formatNameForDatabase(table.getTableName()) + " \n"
				+ "\n"
				+ "\n"
				+ " ) a\n"
				+ "      where ROWNUM <2\n"
				+ "      )\n"
				+ "where rnum  >= 1";
		return sqlForQuery;
	}

}
