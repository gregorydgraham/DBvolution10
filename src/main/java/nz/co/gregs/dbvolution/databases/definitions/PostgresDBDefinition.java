/*
 * Copyright 2013 Gregory Graham.
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

import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import com.vividsolutions.jts.geom.*;
import java.text.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import nz.co.gregs.dbvolution.databases.PostgresDB;
import nz.co.gregs.dbvolution.databases.PostgresDBOverSSL;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.spatial2D.Line2DExpression;
import nz.co.gregs.dbvolution.expressions.spatial2D.MultiPoint2DExpression;
import nz.co.gregs.dbvolution.expressions.spatial2D.Polygon2DExpression;
import nz.co.gregs.dbvolution.internal.postgres.*;
import nz.co.gregs.dbvolution.results.ExpressionHasStandardStringResult;
import nz.co.gregs.dbvolution.utility.TemporalStringParser;
import nz.co.gregs.separatedstring.SeparatedString;
import nz.co.gregs.separatedstring.SeparatedStringBuilder;

/**
 * Defines the features of the PostgreSQL database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link PostgresDB} and
 * {@link PostgresDBOverSSL} instances, and you should not need to use it
 * directly.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class PostgresDBDefinition extends DBDefinition {

	public static final long serialVersionUID = 1L;

	private final DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS Z");

	private static final String[] RESERVED_WORD_ARRAY = new String[]{"A", "ABORT", "ABS", "ABSENT", "ABSOLUTE", "ACCESS", "ACCORDING", "ACTION", "ADA", "ADD", "ADMIN", "AFTER", "AGGREGATE", "ALL", "ALLOCATE", "ALSO", "ALTER", "ALWAYS", "ANALYSE", "ANALYZE", "AND", "ANY", "ARE", "ARRAY", "ARRAY_AGG", "ARRAY_MAX_CARDINALITY", "AS", "ASC", "ASENSITIVE", "ASSERTION", "ASSIGNMENT", "ASYMMETRIC", "AT", "ATOMIC", "ATTACH", "ATTRIBUTE", "ATTRIBUTES", "AUTHORIZATION", "AVG", "BACKWARD", "BASE64", "BEFORE", "BEGIN", "BEGIN_FRAME", "BEGIN_PARTITION", "BERNOULLI", "BETWEEN", "BIGINT", "BINARY", "BIT", "BIT_LENGTH", "BLOB", "BLOCKED", "BOM", "BOOLEAN", "BOTH", "BREADTH", "BY", "C", "CACHE", "CALL", "CALLED", "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CATALOG_NAME", "CEIL", "CEILING", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS", "CHARACTERS", "CHARACTER_LENGTH", "CHARACTER_SET_CATALOG", "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA", "CHAR_LENGTH", "CHECK", "CHECKPOINT", "CLASS", "CLASS_ORIGIN", "CLOB", "CLOSE", "CLUSTER", "COALESCE", "COBOL", "COLLATE", "COLLATION", "COLLATION_CATALOG", "COLLATION_NAME", "COLLATION_SCHEMA", "COLLECT", "COLUMN", "COLUMNS", "COLUMN_NAME", "COMMAND_FUNCTION", "COMMAND_FUNCTION_CODE", "COMMENT", "COMMENTS", "COMMIT", "COMMITTED", "CONCURRENTLY", "CONDITION", "CONDITION_NUMBER", "CONFIGURATION", "CONFLICT", "CONNECT", "CONNECTION", "CONNECTION_NAME", "CONSTRAINT", "CONSTRAINTS", "CONSTRAINT_CATALOG", "CONSTRAINT_NAME", "CONSTRAINT_SCHEMA", "CONSTRUCTOR", "CONTAINS", "CONTENT", "CONTINUE", "CONTROL", "CONVERSION", "CONVERT", "COPY", "CORR", "CORRESPONDING", "COST", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CSV", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_ROW", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CURSOR_NAME", "CYCLE", "DATA", "DATABASE", "DATALINK", "DATE", "DATETIME_INTERVAL_CODE", "DATETIME_INTERVAL_PRECISION", "DAY", "DB", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFAULTS", "DEFERRABLE", "DEFERRED", "DEFINED", "DEFINER", "DEGREE", "DELETE", "DELIMITER", "DELIMITERS", "DENSE_RANK", "DEPENDS", "DEPTH", "DEREF", "DERIVED", "DESC", "DESCRIBE", "DESCRIPTOR", "DETACH", "DETERMINISTIC", "DIAGNOSTICS", "DICTIONARY", "DISABLE", "DISCARD", "DISCONNECT", "DISPATCH", "DISTINCT", "DLNEWCOPY", "DLPREVIOUSCOPY", "DLURLCOMPLETE", "DLURLCOMPLETEONLY", "DLURLCOMPLETEWRITE", "DLURLPATH", "DLURLPATHONLY", "DLURLPATHWRITE", "DLURLSCHEME", "DLURLSERVER", "DLVALUE", "DO", "DOCUMENT", "DOMAIN", "DOUBLE", "DROP", "DYNAMIC", "DYNAMIC_FUNCTION", "DYNAMIC_FUNCTION_CODE", "EACH", "ELEMENT", "ELSE", "EMPTY", "ENABLE", "ENCODING", "ENCRYPTED", "END", "END-EXEC", "END_FRAME", "END_PARTITION", "ENFORCED", "ENUM", "EQUALS", "ESCAPE", "EVENT", "EVERY", "EXCEPT", "EXCEPTION", "EXCLUDE", "EXCLUDING", "EXCLUSIVE", "EXEC", "EXECUTE", "EXISTS", "EXP", "EXPLAIN", "EXPRESSION", "EXTENSION", "EXTERNAL", "EXTRACT", "FALSE", "FAMILY", "FETCH", "FILE", "FILTER", "FINAL", "FIRST", "FIRST_VALUE", "FLAG", "FLOAT", "FLOOR", "FOLLOWING", "FOR", "FORCE", "FOREIGN", "FORTRAN", "FORWARD", "FOUND", "FRAME_ROW", "FREE", "FREEZE", "FROM", "FS", "FULL", "FUNCTION", "FUNCTIONS", "FUSION", "G", "GENERAL", "GENERATED", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GRANTED", "GREATEST", "GROUP", "GROUPING", "GROUPS", "HANDLER", "HAVING", "HEADER", "HEX", "HIERARCHY", "HOLD", "HOUR", "IDENTITY", "IF", "IGNORE", "ILIKE", "IMMEDIATE", "IMMEDIATELY", "IMMUTABLE", "IMPLEMENTATION", "IMPLICIT", "IMPORT", "IN", "INCLUDE", "INCLUDING", "INCREMENT", "INDENT", "INDEX", "INDEXES", "INDICATOR", "INHERIT", "INHERITS", "INITIALLY", "INLINE", "INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INSTANCE", "INSTANTIABLE", "INSTEAD", "INT", "INTEGER", "INTEGRITY", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "INVOKER", "IS", "ISNULL", "ISOLATION", "JOIN", "K", "KEY", "KEY_MEMBER", "KEY_TYPE", "LABEL", "LAG", "LANGUAGE", "LARGE", "LAST", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEAKPROOF", "LEAST", "LEFT", "LENGTH", "LEVEL", "LIBRARY", "LIKE", "LIKE_REGEX", "LIMIT", "LINK", "LISTEN", "LN", "LOAD", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOCATION", "LOCATOR", "LOCK", "LOCKED", "LOGGED", "LOWER", "M", "MAP", "MAPPING", "MATCH", "MATCHED", "MATERIALIZED", "MAX", "MAXVALUE", "MAX_CARDINALITY", "MEMBER", "MERGE", "MESSAGE_LENGTH", "MESSAGE_OCTET_LENGTH", "MESSAGE_TEXT", "METHOD", "MIN", "MINUTE", "MINVALUE", "MOD", "MODE", "MODIFIES", "MODULE", "MONTH", "MORE", "MOVE", "MULTISET", "MUMPS", "NAMES", "NAMESPACE", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NESTING", "NEW", "NEXT", "NFC", "NFD", "NFKC", "NFKD", "NIL", "NO", "NONE", "NORMALIZE", "NORMALIZED", "NOT", "NOTHING", "NOTIFY", "NOTNULL", "NOWAIT", "NTH_VALUE", "NTILE", "NULL", "NULLABLE", "NULLIF", "NULLS", "NUMBER", "NUMERIC", "OBJECT", "OCCURRENCES_REGEX", "OCTETS", "OCTET_LENGTH", "OF", "OFF", "OFFSET", "OIDS", "OLD", "ON", "ONLY", "OPEN", "OPERATOR", "OPTION", "OPTIONS", "OR", "ORDER", "ORDERING", "ORDINALITY", "OTHERS", "OUT", "OUTER", "OUTPUT", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING", "OWNED", "OWNER", "P", "PAD", "PARALLEL", "PARAMETER", "PARAMETER_MODE", "PARAMETER_NAME", "PARAMETER_ORDINAL_POSITION", "PARAMETER_SPECIFIC_CATALOG", "PARAMETER_SPECIFIC_NAME", "PARAMETER_SPECIFIC_SCHEMA", "PARSER", "PARTIAL", "PARTITION", "PASCAL", "PASSING", "PASSTHROUGH", "PASSWORD", "PATH", "PERCENT", "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "PERIOD", "PERMISSION", "PLACING", "PLANS", "PLI", "POLICY", "PORTION", "POSITION", "POSITION_REGEX", "POWER", "PRECEDES", "PRECEDING", "PRECISION", "PREPARE", "PREPARED", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURAL", "PROCEDURE", "PROCEDURES", "PROGRAM", "PUBLIC", "PUBLICATION", "QUOTE", "RANGE", "RANK", "READ", "READS", "REAL", "REASSIGN", "RECHECK", "RECOVERY", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REFRESH", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY", "REINDEX", "RELATIVE", "RELEASE", "RENAME", "REPEATABLE", "REPLACE", "REPLICA", "REQUIRING", "RESET", "RESPECT", "RESTART", "RESTORE", "RESTRICT", "RESULT", "RETURN", "RETURNED_CARDINALITY", "RETURNED_LENGTH", "RETURNED_OCTET_LENGTH", "RETURNED_SQLSTATE", "RETURNING", "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE", "ROUTINES", "ROUTINE_CATALOG", "ROUTINE_NAME", "ROUTINE_SCHEMA", "ROW", "ROWS", "ROW_COUNT", "ROW_NUMBER", "RULE", "SAVEPOINT", "SCALE", "SCHEMA", "SCHEMAS", "SCHEMA_NAME", "SCOPE", "SCOPE_CATALOG", "SCOPE_NAME", "SCOPE_SCHEMA", "SCROLL", "SEARCH", "SECOND", "SECTION", "SECURITY", "SELECT", "SELECTIVE", "SELF", "SENSITIVE", "SEQUENCE", "SEQUENCES", "SERIALIZABLE", "SERVER", "SERVER_NAME", "SESSION", "SESSION_USER", "SET", "SETOF", "SETS", "SHARE", "SHOW", "SIMILAR", "SIMPLE", "SIZE", "SKIP", "SMALLINT", "SNAPSHOT", "SOME", "SOURCE", "SPACE", "SPECIFIC", "SPECIFICTYPE", "SPECIFIC_NAME", "SQL", "SQLCODE", "SQLERROR", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQRT", "STABLE", "STANDALONE", "START", "STATE", "STATEMENT", "STATIC", "STATISTICS", "STDDEV_POP", "STDDEV_SAMP", "STDIN", "STDOUT", "STORAGE", "STRICT", "STRIP", "STRUCTURE", "STYLE", "SUBCLASS_ORIGIN", "SUBMULTISET", "SUBSCRIPTION", "SUBSTRING", "SUBSTRING_REGEX", "SUCCEEDS", "SUM", "SYMMETRIC", "SYSID", "SYSTEM", "SYSTEM_TIME", "SYSTEM_USER", "T", "TABLE", "TABLES", "TABLESAMPLE", "TABLESPACE", "TABLE_NAME", "TEMP", "TEMPLATE", "TEMPORARY", "TEXT", "THEN", "TIES", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TOKEN", "TOP_LEVEL_COUNT", "TRAILING", "TRANSACTION", "TRANSACTIONS_COMMITTED", "TRANSACTIONS_ROLLED_BACK", "TRANSACTION_ACTIVE", "TRANSFORM", "TRANSFORMS", "TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION", "TREAT", "TRIGGER", "TRIGGER_CATALOG", "TRIGGER_NAME", "TRIGGER_SCHEMA", "TRIM", "TRIM_ARRAY", "TRUE", "TRUNCATE", "TRUSTED", "TYPE", "TYPES", "UESCAPE", "UNBOUNDED", "UNCOMMITTED", "UNDER", "UNENCRYPTED", "UNION", "UNIQUE", "UNKNOWN", "UNLINK", "UNLISTEN", "UNLOGGED", "UNNAMED", "UNNEST", "UNTIL", "UNTYPED", "UPDATE", "UPPER", "URI", "USAGE", "USER", "USER_DEFINED_TYPE_CATALOG", "USER_DEFINED_TYPE_CODE", "USER_DEFINED_TYPE_NAME", "USER_DEFINED_TYPE_SCHEMA", "USING", "VACUUM", "VALID", "VALIDATE", "VALIDATOR", "VALUE", "VALUES", "VALUE_OF", "VARBINARY", "VARCHAR", "VARIADIC", "VARYING", "VAR_POP", "VAR_SAMP", "VERBOSE", "VERSION", "VERSIONING", "VIEW", "VIEWS", "VOLATILE", "WHEN", "WHENEVER", "WHERE", "WHITESPACE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRAPPER", "WRITE", "XML", "XMLAGG", "XMLATTRIBUTES", "XMLBINARY", "XMLCAST", "XMLCOMMENT", "XMLCONCAT", "XMLDECLARATION", "XMLDOCUMENT", "XMLELEMENT", "XMLEXISTS", "XMLFOREST", "XMLITERATE", "XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLQUERY", "XMLROOT", "XMLSCHEMA", "XMLSERIALIZE", "XMLTABLE", "XMLTEXT", "XMLVALIDATE", "YEAR", "YES", "ZONE"};
	private static final List<String> RESERVED_WORD_LIST = Arrays.asList(RESERVED_WORD_ARRAY);

	@Override
	public String getDropDatabase(String databaseName) throws UnsupportedOperationException {
		return "DROP DATABASE IF EXISTS '" + databaseName + "';";
	}

	@Override
	public String formatPrimaryKeyForRetrievingGeneratedKeys(String primaryKeyColumnName) {
		return primaryKeyColumnName.toLowerCase();
	}

	@Override
	protected String formatNameForDatabase(final String sqlObjectName) {
		if (!(RESERVED_WORD_LIST.contains(sqlObjectName.toUpperCase()))) {
			return super.formatNameForDatabase(sqlObjectName);
		} else {
			return formatNameForDatabase("p" + super.formatNameForDatabase(sqlObjectName));
		}
	}

	@Override
	public String getDateFormattedForQuery(Date date) {
		return "(TIMESTAMP '" + DATETIME_FORMAT.format(date) + "')";
	}

	@Override
	public String getLocalDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return SeparatedStringBuilder.startsWith("make_timestamp(").separatedBy(", ").endsWith(")")
				.addAll(years, months, days)
				.addAll(hours, minutes, "(" + seconds + "+" + subsecond + ")")
				//				.add(
				//						doConcatTransform(
				//								"'" + timeZoneSign + "'", timeZoneHourOffset, doRightPadTransform("'" + timeZoneMinuteOffSet + "'", "'0'", "2")
				//						)
				//				)
				.toString();
	}

	@Override
	public String getInstantPartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return SeparatedStringBuilder.startsWith("make_timestamptz(").separatedBy(", ").endsWith(")")
				.addAll(years, months, days)
				.addAll(hours, minutes, "(" + seconds + "+" + subsecond + ")")
				.toString();
	}

	@Override
	public String getDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return SeparatedStringBuilder.startsWith("make_timestamptz(").separatedBy(", ").endsWith(")")
				.addAll(years, months, days)
				.addAll(hours, minutes, "(" + seconds + "+" + subsecond + ")")
				.add(
						doConcatTransform(
								"'" + timeZoneSign + "'", doLeftPadTransform("'" + timeZoneHourOffset + "'", "'0'", "'2'"), doRightPadTransform("'" + timeZoneMinuteOffSet + "'", "'0'", "2")
						)
				)
				.toString();
	}

	@Override
	public String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		if (qdt instanceof DBLargeText) {
			return " TEXT ";
		} else if (qdt instanceof DBInteger) {
			return " BIGINT ";
		} else if (qdt instanceof DBDate) {
			return " TIMESTAMP ";
		} else if (qdt instanceof DBInstant) {
			return " TIMESTAMPTZ ";
		} else if (qdt instanceof DBLargeObject) {
			return " BYTEA ";
		} else if (qdt instanceof DBBoolean) {
			return " BOOLEAN ";
		} else if (qdt instanceof DBBooleanArray) {
			return " BOOL[] ";
		} else if (qdt instanceof DBLine2D) {
			return " PATH ";
		} else if (qdt instanceof DBLineSegment2D) {
			return " PATH ";
		} else if (qdt instanceof DBMultiPoint2D) {
			return " GEOMETRY ";
		} else if (qdt instanceof DBDuration) {
			return " INTERVAL DAY TO SECOND(6) ";
		} else {
			return super.getDatabaseDataTypeOfQueryableDatatype(qdt);
		}
	}

	@Override
	public Class<? extends QueryableDatatype<?>> getQueryableDatatypeClassForSQLDatatype(String typeName) {
		switch (typeName) {
			case "path":
				return DBLine2D.class;
			case "geometry":
				return DBMultiPoint2D.class;
			default:
				return super.getQueryableDatatypeClassForSQLDatatype(typeName);
		}
	}

//	@Override
//	public String getOrderByDirectionClause(Boolean sortOrder) {
//		if (sortOrder == null) {
//			return "";
//		} else if (sortOrder) {
//			return " ASC NULLS FIRST ";
//		} else {
//			return " DESC NULLS LAST ";
//		}
//	}
	@Override
	public String getOrderByDescending() {
		return " DESC ";
	}

	@Override
	public String getOrderByAscending() {
		return " ASC ";
	}

	@Override
	public String doTruncTransform(String firstString, String secondString) {
		return getTruncFunctionName() + "((" + firstString + ")::numeric, " + secondString + ")";
	}

	@Override
	public String doNumberToIntegerTransform(String sql) {
		return "CAST((" + sql + ") as INTEGER)";
	}

//	@Override
//	public String doBooleanToIntegerTransform(String columnName) {
//		return "case when " + columnName + " is null then null when " + columnName + " then 1 else 0 end";
////		return "(" + columnName + ")::integer";
//	}
	@Override
	public String doIntegerToBitTransform(String columnName) {
		return columnName + "::bit";
	}

	@Override
	public String doBitsValueTransform(boolean[] boolArray) {
		StringBuilder boolStr = new StringBuilder();
		for (boolean c : boolArray) {
			if (c) {
				boolStr.append("1");
			} else {
				boolStr.append("0");
			}
		}
		return "B'" + boolStr.toString() + "'";
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return "";
	}

	@Override
	protected boolean hasSpecialAutoIncrementType() {
		return true;
	}

	@Override
	protected String getSpecialAutoIncrementType() {
		return " SERIAL ";
	}

	@Override
	public boolean supportsModulusFunction() {
		return false;
	}

	@Override
	public String doSubsecondTransform(String dateExpression) {
		return "((EXTRACT(MILLISECOND FROM " + dateExpression + ")/1000.0000) - (" + doTruncTransform(doSecondTransform(dateExpression), "0") + "))";
	}

//	@Override
//	public String doAddMillisecondsTransform(String dateValue, String numberOfSeconds) {
//		return "(" + dateValue + "+ (" + numberOfSeconds + ")*INTERVAL '1 MILLISECOND' )";
//	}
	@Override
	public String doDateAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + "+ (" + numberOfSeconds + ")*INTERVAL '1 SECOND' )";
	}

	@Override
	public String doDateAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "(" + dateValue + "+ (" + numberOfMinutes + ")*INTERVAL '1 MINUTE' )";
	}

	@Override
	public String doDateAddDaysTransform(String dateValue, String numberOfDays) {
		return "(" + dateValue + "+ (" + numberOfDays + ")*INTERVAL '1 DAY' )";
	}

	@Override
	public String doDateAddHoursTransform(String dateValue, String numberOfHours) {
		return "(" + dateValue + "+ (" + numberOfHours + ")*INTERVAL '1 HOUR')";
	}

	@Override
	public String doDateAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 WEEK')";
	}

	@Override
	public String doDateAddMonthsTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 MONTH')";
	}

	@Override
	public String doDateAddYearsTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*(INTERVAL '1 YEAR'))";
	}

	@Override
	public String doInstantAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return "(" + dateValue + "+ (" + numberOfSeconds + ")*INTERVAL '1 SECOND' )";
	}

	@Override
	public String doInstantAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return "(" + dateValue + "+ (" + numberOfMinutes + ")*INTERVAL '1 MINUTE' )";
	}

	@Override
	public String doInstantAddDaysTransform(String dateValue, String numberOfDays) {
		return "(" + dateValue + "+ (" + numberOfDays + ")*INTERVAL '1 DAY' )";
	}

	@Override
	public String doInstantAddHoursTransform(String dateValue, String numberOfHours) {
		return "(" + dateValue + "+ (" + numberOfHours + ")*INTERVAL '1 HOUR')";
	}

	@Override
	public String doInstantAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 WEEK')";
	}

	@Override
	public String doInstantAddMonthsTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*INTERVAL '1 MONTH')";
	}

	@Override
	public String doInstantAddYearsTransform(String dateValue, String numberOfWeeks) {
		return "(" + dateValue + "+ (" + numberOfWeeks + ")*(INTERVAL '1 YEAR'))";
	}

	@Override
	public String doBooleanValueTransform(Boolean boolValue) {
		return "" + (boolValue ? 1 : 0) + "::bool";
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return super.getCurrentDateOnlyFunctionName(); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return "(EXTRACT(DAY from (" + otherDateValue + ")-(" + dateValue + ")))";
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND(" + doDayDifferenceTransform(dateValue, otherDateValue) + "/7)";
	}

	private String doAgeTransformation(String dateValue, String otherDateValue) {
		return "age((" + dateValue + "), (" + otherDateValue + "))";
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return "ROUND(EXTRACT(YEAR FROM " + doAgeTransformation(dateValue, otherDateValue) + ") * 12 + EXTRACT(MONTH FROM " + doAgeTransformation(dateValue, otherDateValue) + ")*-1)";
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(YEAR FROM " + doAgeTransformation(dateValue, otherDateValue) + ")*-1)";
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + ")) / -3600)";
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + ")) / -60)";
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + "))*-1)";
	}

//	@Override
//	public String doMillisecondDifferenceTransform(String dateValue, String otherDateValue) {
//		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + "))*-1000)";
//	}
	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return " (EXTRACT(DOW FROM (" + dateSQL + "))+1)";
	}

//	@Override
//	public String doMillisecondDifferenceTransform(String dateValue, String otherDateValue) {
//		return "round(EXTRACT(EPOCH FROM (" + dateValue + ") - (" + otherDateValue + "))*-1000)";
//	}
	@Override
	public String doInstantDayOfWeekTransform(String dateSQL) {
//		return " (EXTRACT(DOW FROM (" + dateSQL + "))+1)";
		return " (EXTRACT(DOW FROM (" + dateSQL + ") AT TIME ZONE 'UTC')+1)";
	}

	@Override
	public String doBooleanArrayTransform(Boolean[] bools) {
		StringBuilder str = new StringBuilder();
		str.append("'{");
		String separator = "";
		switch (bools.length) {
			case 0:
				return "'{}'";
			case 1:
				return "'{" + bools[0] + "}'";
			default:
				for (Boolean bool : bools) {
					str.append(separator).append(bool ? 1 : 0);
					separator = ",";
				}
				str.append("}'");
				return str.toString();
		}
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return "ST_INTERSECTION((" + firstGeometry + ")::GEOMETRY,  (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
//		return "(" + firstGeometry + ") ?#  (" + secondGeometry + ")";
		return "ST_OVERLAPS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
//		return "(" + firstGeometry + ") ?#  (" + secondGeometry + ")";
		return "((" + firstGeometry + ") && (" + secondGeometry + "))";
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return "ST_TOUCHES((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DGetAreaTransform(String toSQLString) {
		return "ST_AREA((" + toSQLString + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String toSQLString) {
		return "BOX2D(" + toSQLString + "::GEOMETRY)";
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return "ST_EQUALS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return "ST_CONTAINS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String firstGeometry, String secondGeometry) {
		return "ST_CONTAINS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return "ST_DISJOINT((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		//indicate whether g1 is spatially within g2. This is the inverse of Contains(). 
		// i.e. G1.within(G2) === G2.contains(G1)
		return "ST_WITHIN((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String toSQLString) {
		return "ST_DIMENSION((" + toSQLString + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		return "PATH(ST_EXTERIORRING((" + polygon2DSQL + ")::GEOMETRY))";
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		return "ST_XMAX((" + polygon2DSQL + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		return "ST_XMIN((" + polygon2DSQL + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		return "ST_YMAX((" + polygon2DSQL + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		return "ST_YMIN((" + polygon2DSQL + ")::GEOMETRY)";
	}

	@Override
	public String doPolygon2DAsTextTransform(String toSQLString) {
		return "ST_ASTEXT((" + toSQLString + ")::GEOMETRY)";
	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return false;//To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return "(CASE WHEN (" + stringResultContainingANumber + ") IS NULL OR (" + stringResultContainingANumber + ") = '' THEN NULL ELSE TO_NUMBER(" + stringResultContainingANumber + ", 'S999999999999999D9999999') END)";
	}

	@Override
	public boolean supportsArcSineFunction() {
		return false;
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return "POINT (" + xValue + ", " + yValue + ")";
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return "((" + firstPoint + ")~=( " + secondPoint + "))";
	}

	@Override
	public String doPoint2DGetXTransform(String point2D) {
		return "(" + point2D + ")[0]";
	}

	@Override
	public String doPoint2DGetYTransform(String point2D) {
		return "(" + point2D + ")[1]";
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String point2D) {
		return " 0 ";
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2D) {
		return "POLYGON(BOX(" + point2D + "," + point2D + "))";
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DString) {
		return "(" + point2DString + ")::VARCHAR";
	}

	@Override
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		return "POINT (" + point.getX() + ", " + point.getY() + ")";
	}

	//path '[(0,0),(1,1),(2,0)]'
	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString line) {
		StringBuilder str = new StringBuilder();
		String separator = "";
		Coordinate[] coordinates = line.getCoordinates();
		for (Coordinate coordinate : coordinates) {
			str.append(separator).append("(").append(coordinate.x).append(",").append(coordinate.y).append(")");
			separator = ",";
		}
		return "PATH '[" + str + "]'";
	}

	@Override
	public String doColumnTransformForSelect(QueryableDatatype<?> qdt, String selectableName) {
		if (qdt instanceof DBPolygon2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBPoint2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBLine2D) {
			return "(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBMultiPoint2D) {
			return "ST_ASTEXT(" + selectableName + ")::VARCHAR";
		} else if (qdt instanceof DBInstant) {
			return "(" + selectableName + ") at time zone 'UTC'";
//			return "to_char((" + selectableName + ") at time zone 'UTC', 'YYYY-MM-DD HH:MI:SS.US')";
		} else {
			return super.doColumnTransformForSelect(qdt, selectableName);
			//return selectableName;
		}
	}

	@Override
	public Point transformDatabasePoint2DValueToJTSPoint(String pointAsString) throws com.vividsolutions.jts.io.ParseException {
		Point point = null;
		if (pointAsString.matches(" *\\( *[-0-9.]+, *[-0-9.]+ *\\) *")) {
			String[] split = pointAsString.split("[^-0-9.]+");

			GeometryFactory geometryFactory = new GeometryFactory();
			final double x = Double.parseDouble(split[1]);
			final double y = Double.parseDouble(split[2]);
			point = geometryFactory.createPoint(new Coordinate(x, y));
		} else {
//			throw new IncorrectGeometryReturnedForDatatype(geometry, point);
		}
		return point;
	}

	// ((2,3),(2,3),(2,3),(2,3))
	// POLYGON((2 3,2 4,3 4,3 3,2 3))
	@Override
	public Polygon transformDatabasePolygon2DToJTSPolygon(String geometryAsString) throws com.vividsolutions.jts.io.ParseException {
		//String string = "POLYGON " + geometryAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
		String[] splits = geometryAsString.split("[^0-9.]+");
		List<Coordinate> coords = new ArrayList<>();
		Coordinate firstCoord = null;
		for (int i = 1; i < splits.length; i += 2) {
			String splitX = splits[i];
			String splitY = splits[i + 1];
			final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
			coords.add(coordinate);
			if (firstCoord == null) {
				firstCoord = coordinate;
			}
		}
		if (coords.size() == 1) {
			coords.add(firstCoord);
			coords.add(firstCoord);
			coords.add(firstCoord);
			coords.add(firstCoord);
		}
		final GeometryFactory geometryFactory = new GeometryFactory();
		Polygon polygon = geometryFactory.createPolygon(coords.toArray(new Coordinate[]{}));
		return polygon;
	}

	@Override
	public LineString transformDatabaseLine2DValueToJTSLineString(String lineStringAsString) throws com.vividsolutions.jts.io.ParseException {
		LineString lineString;
		if (!lineStringAsString.matches("^ *LINESTRING.*")) {
			//String string = "LINESTRING " + lineStringAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
			String[] splits = lineStringAsString.split("[(),]+");
			Coordinate firstCoord = null;
			List<Coordinate> coords = new ArrayList<>();
			for (int i = 1; i < splits.length - 1; i += 2) {
				String splitX = splits[i];
				String splitY = splits[i + 1];
				final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
				coords.add(coordinate);
				if (firstCoord == null) {
					firstCoord = coordinate;
				}
			}
			final GeometryFactory geometryFactory = new GeometryFactory();
			lineString = geometryFactory.createLineString(coords.toArray(new Coordinate[]{}));
		} else {
			return super.transformDatabaseLine2DValueToJTSLineString(lineStringAsString);
		}
		return lineString;
	}

	@Override
	public String doLine2DEqualsTransform(String firstLineSQL, String secondLineSQL) {
		return "(" + firstLineSQL + ")::TEXT = (" + secondLineSQL + ")::TEXT";
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return "(" + line2DSQL + ")::TEXT";
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String toSQLString) {
		return Line2DFunctions.BOUNDINGBOX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY + "(" + toSQLString + ")";
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String firstGeometry, String secondGeometry) {
		return "ST_INTERSECTS((" + firstGeometry + ")::GEOMETRY , (" + secondGeometry + ")::GEOMETRY)";
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return Line2DFunctions.INTERSECTIONWITHLINE2D + "((" + firstGeometry + ") , (" + secondGeometry + "))";
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstGeometry, String secondGeometry) {
		return Line2DFunctions.INTERSECTIONPOINTSWITHLINE2D + "((" + firstGeometry + ") , (" + secondGeometry + "))";
	}

	@Override
	public String doSubstringBeforeTransform(String fromThis, String beforeThis) {
		return StringFunctions.SUBSTRINGBEFORE + "(" + fromThis + ", " + beforeThis + ")";
	}

	@Override
	public String doSubstringAfterTransform(String fromThis, String afterThis) {
		return StringFunctions.SUBSTRINGAFTER + "(" + fromThis + ", " + afterThis + ")";
	}

	//INSERT INTO BasicSpatialTable( myfirstgeom)  VALUES ( polygon 'POLYGON ((5 10, 6 10, 6 11, 5 11, 5 10))')
	//INSERT INTO BasicSpatialTable( myfirstgeom)  VALUES ( polygon '((5,10), (6,10), (6,11), (5,11), (5,10))');
	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon polygon) {

		StringBuilder str = new StringBuilder();
		String separator = "";
		Coordinate[] coordinates = polygon.getCoordinates();
		for (Coordinate coordinate : coordinates) {
			str.append(separator).append("(").append(coordinate.x).append(",").append(coordinate.y).append(")");
			separator = ",";
		}

		return "POLYGON '(" + str + ")'";
	}

	@Override
	public String transformCoordinateArrayToDatabasePolygon2DFormat(List<String> coordinateSQL) {

		StringBuilder str = new StringBuilder();
		String separator = "";
		for (String coordinate : coordinateSQL) {
			str.append(separator).append(coordinate);
			separator = ",";
		}

		return "POLYGON '(" + str + ")'";
	}

	@Override
	public String transformPoint2DArrayToDatabasePolygon2DFormat(List<String> pointSQL) {
		//POINT (0.0, 0.0) => POLYGON((0.0, 0.0), ... )
		StringBuilder str = new StringBuilder();
		String separator = "";
		for (String point : pointSQL) {
			final String coordsOnly = point.replaceAll("POINT ", "");
			str.append(separator).append(coordsOnly);
			separator = ",";
		}

		return "POLYGON '(" + str + ")'";
	}

	@Override
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		if (columnExpression instanceof Line2DExpression) {
			return ((ExpressionHasStandardStringResult) columnExpression).stringResult();
		} else if (columnExpression instanceof MultiPoint2DExpression) {
			return ((ExpressionHasStandardStringResult) columnExpression).stringResult();
		} else if (columnExpression instanceof Polygon2DExpression) {
			return ((ExpressionHasStandardStringResult) columnExpression).stringResult();
		} else {
			return super.transformToStorableType(columnExpression);
		}
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		LineString toGeometry = lineSegment.toGeometry(new GeometryFactory());
		return transformLineStringIntoDatabaseLine2DFormat(toGeometry);
	}

	@Override
	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineStringAsString) throws com.vividsolutions.jts.io.ParseException {
		LineSegment lineString;
		if (!lineStringAsString.matches("^ *LINESTRING.*")) {
			//String string = "LINESTRING " + lineStringAsString.replaceAll("\\),\\(", ", ").replaceAll("([-0-9.]+),([-0-9.]+)", "$1 $2");
			String[] splits = lineStringAsString.split("[(),]+");
			Coordinate firstCoord = null;
			List<Coordinate> coords = new ArrayList<>();
			for (int i = 1; i < splits.length - 1; i += 2) {
				String splitX = splits[i];
				String splitY = splits[i + 1];
				final Coordinate coordinate = new Coordinate(Double.parseDouble(splitX), Double.parseDouble(splitY));
				coords.add(coordinate);
				if (firstCoord == null) {
					firstCoord = coordinate;
				}
			}
			coords.add(firstCoord);
			lineString = new LineSegment(coords.get(0), coords.get(1));
		} else {
			return super.transformDatabaseLineSegment2DValueToJTSLineSegment(lineStringAsString);
		}
		return lineString;
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String toSQLString, String toSQLString0) {
		return "ST_INTERSECTS((" + toSQLString + ")::GEOMETRY , (" + toSQLString0 + ")::GEOMETRY)";
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String toSQLString) {
		return Line2DFunctions.MAXX + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String toSQLString) {
		return Line2DFunctions.MINX + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String toSQLString) {
		return Line2DFunctions.MAXY + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String toSQLString) {
		return Line2DFunctions.MINY + "(" + toSQLString + ")";
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String toSQLString) {
		return Line2DFunctions.BOUNDINGBOX + "((" + toSQLString + ")::PATH)";
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String toSQLString, String toSQLString0) {
		return "((" + toSQLString + "):TEXT <> (" + toSQLString0 + ")::TEXT)";
	}

	@Override
	public String doLineSegment2DEqualsTransform(String toSQLString, String toSQLString0) {
		return "((" + toSQLString + ")::TEXT = (" + toSQLString0 + ")::TEXT)";
	}

	@Override
	public String doLineSegment2DAsTextTransform(String toSQLString) {
		return "(" + toSQLString + ")::TEXT";
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return "ST_PointFROMTEXT(ST_ASTEXT(ST_INTERSECTION((" + firstLineSegment + ")::GEOMETRY , (" + secondLineSegment + ")::GEOMETRY)))::POINT";
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		return super.transformMultiPoint2DToDatabaseMultiPoint2DValue(points);
	}

	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		return super.transformDatabaseMultiPoint2DValueToJTSMultiPoint(pointsAsString);
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String first, String second) {
		return "ST_EQUALS(" + first + ", " + second + ")";
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return "ST_GEOMETRYN(" + first + ", " + index + ")::POINT";
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String first) {
		return "ST_NPOINTS(" + first + ")";
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String first) {
		return "ST_DIMENSION(" + first + ")";
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String first) {
		return "ST_REVERSE(ST_ENVELOPE(" + first + "))";
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String first) {
		return "ST_ASTEXT((" + first + ")::GEOMETRY)::TEXT";
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String first) {
		return MultiPoint2DFunctions.ASLINE2D + "((" + first + "))::PATH";
	}

//	@Override
//	public String doMultiPoint2DToPolygon2DTransform(String first) {
//		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
//	}
	@Override
	public String doMultiPoint2DGetMinYTransform(String toSQLString) {
		return "ST_YMIN(" + toSQLString + ")";
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String toSQLString) {
		return "ST_XMIN(" + toSQLString + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String toSQLString) {
		return "ST_YMAX(" + toSQLString + ")";
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String toSQLString) {
		return "ST_XMAX(" + toSQLString + ")";
	}

	// Relies on Java8 :(
//	@Override
//	public String doDateAtTimeZoneTransform(String dateSQL, TimeZone timeZone) {
//		return "((" + dateSQL + ") AT TIME ZONE '" + timeZone.toZoneId().getId() + "') ";
//	}
	@Override
	public LargeObjectHandlerType preferredLargeObjectWriter(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeBinary) {
			return LargeObjectHandlerType.BINARYSTREAM;
		} else if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.CHARSTREAM;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BINARYSTREAM;
		} else {
			return super.preferredLargeObjectWriter(lob);
		}
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectReader(DBLargeObject<?> lob) {
		if (lob instanceof DBLargeBinary) {
			return LargeObjectHandlerType.BINARYSTREAM;
		} else if (lob instanceof DBLargeText) {
			return LargeObjectHandlerType.STRING;
		} else if (lob instanceof DBJavaObject) {
			return LargeObjectHandlerType.BINARYSTREAM;
		} else {
			return super.preferredLargeObjectReader(lob);
		}
	}

	/**
	 * Return the function name for the Logarithm Base10 function.
	 *
	 * <p>
	 * By default this method returns <b>log10</b></p>
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name of the function to use when rounding numbers up
	 */
	@Override
	public String getLogBase10FunctionName() {
		return "log";
	}

	@Override
	public String doLogBase10NumberTransform(String sql) {
		return "log(" + sql + ")";
	}

	@Override
	public String doFindNumberInStringTransform(String toSQLString) {
		return "(substring(" + toSQLString + " from '([-]?[0-9]+(\\.[0-9]+)?)'))";
	}

	@Override
	public String doFindIntegerInStringTransform(String toSQLString) {
		return "(substring(" + toSQLString + " from '([-]?[0-9]+)'))";
	}

	@Override
	public String doRandomNumberTransform() {
		return " (RANDOM())";
	}

	@Override
	public boolean supportsTableCheckingViaMetaData() {
		return false;
	}

	@Override
	public boolean requiresOnClauseForAllJoins() {
		return true;
	}

	@Override
	public boolean requiresSequenceUpdateAfterManualInsert() {
		return true;
	}

	@Override
	public String getSequenceUpdateSQL(String tableName, String columnName, long primaryKeyGenerated) {
		return "ALTER SEQUENCE " + tableName + "_" + columnName + "_seq RESTART WITH " + (primaryKeyGenerated + 1) + ";";
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String referencedTable) {
		return "STRING_AGG(" + accumulateColumn + ", " + separator + ")";
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String orderByColumnName, String referencedTable) {
		return "STRING_AGG(" + accumulateColumn + ", " + separator + " ORDER BY " + orderByColumnName + ")";
	}

	/**
	 * Generate the SQL to apply rounding to the Number expressions with the
	 * specified number of decimal places.
	 *
	 * @param number the number value
	 * @param decimalPlaces the number value of the decimal places required.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	@Override
	public String doRoundWithDecimalPlacesTransform(String number, String decimalPlaces) {
		return "ROUND((" + number + ")::numeric, " + decimalPlaces + ")";
	}

	/**
	 * Creates the CURRENTTIME function for this database.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the default implementation returns " CURRENT_TIMESTAMP "
	 */
	@Override
	public String doCurrentUTCTimeTransform() {
		return "(now())";
//		return "(now() at time zone 'utc')";
	}

	@Override
	public String doCurrentUTCDateTimeTransform() {
		return "(now())";
//		return "(now() at time zone 'utc')";
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the year part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a SQL snippet that will produce the year of the supplied date.
	 */
	@Override
	public String doInstantYearTransform(String dateExpression) {
//		return doNumberToIntegerTransform("EXTRACT(YEAR FROM " + dateExpression + ")");
		return doNumberToIntegerTransform("EXTRACT(YEAR FROM " + dateExpression + " AT TIME ZONE 'UTC')");
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the month part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a SQL snippet that will produce the month of the supplied date.
	 */
	@Override
	public String doInstantMonthTransform(String dateExpression) {
//		return doNumberToIntegerTransform("EXTRACT(MONTH FROM " + dateExpression + ")");
		return doNumberToIntegerTransform("EXTRACT(MONTH FROM " + dateExpression + " AT TIME ZONE 'UTC')");
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the day part of the date.
	 *
	 * <p>
	 * Day in this sense is the number of the day within the month: that is the 23
	 * part of Monday 25th of August 2014
	 *
	 * @param dateExpression	dateExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a SQL snippet that will produce the day of the supplied date.
	 */
	@Override
	public String doInstantDayTransform(String dateExpression) {
//		return doNumberToIntegerTransform("EXTRACT(DAY FROM " + dateExpression + ")");
		return doNumberToIntegerTransform("EXTRACT(DAY FROM " + dateExpression + " AT TIME ZONE 'UTC')");
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the hour part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a SQL snippet that will produce the hour of the supplied date.
	 */
	@Override
	public String doInstantHourTransform(String dateExpression) {
//		return doNumberToIntegerTransform("EXTRACT(HOUR FROM " + dateExpression + ")");
		return doNumberToIntegerTransform("EXTRACT(HOUR FROM " + dateExpression + " AT TIME ZONE 'UTC')");
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the minute part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a SQL snippet that will produce the minute of the supplied date.
	 */
	@Override
	public String doInstantMinuteTransform(String dateExpression) {
//		return doNumberToIntegerTransform("EXTRACT(MINUTE FROM " + dateExpression + ")");
		return doNumberToIntegerTransform("EXTRACT(MINUTE FROM " + dateExpression + " AT TIME ZONE 'UTC')");
	}

	/**
	 * Transforms a SQL snippet that is assumed to be a date into an SQL snippet
	 * that provides the second part of the date.
	 *
	 * @param dateExpression	dateExpression
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a SQL snippet that will produce the second of the supplied date.
	 */
	@Override
	public String doInstantSecondTransform(String dateExpression) {
//		return "EXTRACT(SECOND FROM " + dateExpression + ")";
		return "EXTRACT(SECOND FROM " + dateExpression + " AT TIME ZONE 'UTC')";
	}

	/**
	 * Returns the partial second value from the date.
	 *
	 * <p>
	 * This should return the most detailed possible value less than a second for
	 * the date expression provided. It should always return a value less than 1s.
	 *
	 * @param dateExpression the date from which to get the subsecond part of.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return SQL
	 */
	@Override
	public String doInstantSubsecondTransform(String dateExpression) {
//		return "(EXTRACT(MILLISECOND FROM " + dateExpression + ")/1000.0000)";
		return "(EXTRACT(MILLISECOND FROM " + dateExpression + " AT TIME ZONE 'UTC')/1000.0000)";
	}

	@Override
	public String doInstantEndOfMonthTransform(String dateSQL) {
//		return super.doInstantEndOfMonthTransform(dateSQL);
		return "(((" + dateSQL + " at time zone 'UTC'+ (( CAST((EXTRACT(DAY FROM " + dateSQL + " AT TIME ZONE 'UTC')) as INTEGER) - 1)  * -1)*INTERVAL '1 DAY' )+ (1)*INTERVAL '1 MONTH')+ (-1)*INTERVAL '1 DAY' ) at time zone 'UTC'";
	}

	@Override
	public boolean prefersInstantsReadAsStrings() {
		return true;
	}

	//2013-03-24 01:34:56+13
	@Override
	public Instant parseInstantFromGetString(String inputFromResultSet) throws ParseException {
		return TemporalStringParser.toInstant(inputFromResultSet);
	}
	//2013-03-24 01:34:56+13

	@Override
	public DBExpression transformToSelectableType(DBExpression columnExpression) {
		return super.transformToSelectableType(columnExpression);
	}
	
	@Override
	public int getParseDurationPartOffset() {
		return 0;
	}
}
