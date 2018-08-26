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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.exceptions.UnableToAccessDBReportFieldException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBReportSubclassException;
import nz.co.gregs.dbvolution.exceptions.UnableToSetDBReportFieldException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * DBReport provides support for defining a complex query permanently.
 *
 * <p>
 * Use a DBReport rather than a {@link DBQuery} when the query is expected to be
 * reused frequently and needs to be well defined. A similar effect can be
 * achieved by defining a method that returns the DBQuery required, however
 * DBReport provides a cleaner syntax.
 *
 * <p>
 * DBReport functions similarly to {@link DBRow}. In particular you need to
 * subclass DBReport to define and provide the required columns.
 * <p>
 * However, DBReport allows you to add DBRows, expression columns, and
 * conditions to the class, combining several tables in your query.
 *
 * <p>
 * Like DBRow you will need to define the columns required. The difference is
 * that DBReport columns should be expressions based on the DBRows in the
 * report, rather than direct database column references
 *
 * <p>
 * A primary feature of DBReport is automatic grouping of the results when
 * aggregators are included. Aggregator functions, like AVERAGE, operate over
 * several rows and require the rows to be grouped by the other columns.
 * DBReport automatically manages the GROUP BY clause to avoid any issues.
 *
 * <p>
 * Creating a DBReport follows a simple pattern:
 * <ul>
 * <li>Create a class that extends DBReport<br>
 * {@code public class SimpleReport extends DBReport}
 * <li>Add some DBRows as fields:<br>
 * {@code public Marque marque = new Marque();}<br>
 * {@code public CarCompany carCompany = new CarCompany();}<br>
 * <li>Define some columns:<br>
 * {@code @DBColumn public DBString carCompanyName = new DBString(carCompany.column(carCompany.name));}<br>
 * {@code @DBColumn public DBNumber numberOfMarques = new DBNumber(NumberExpression.countAll());}<br>
 * <li>Use an initialization block to define some required properties<br>
 * {@code {carCompany.uidCarCompany.excludedValues((Long) null);}}<br>
 * </ul>
 *
 * <p>
 * Retrieving a DBReport is easily accomplished using the
 * {@link DBDatabase#get(DBReport, DBRow...) DBDatabase get(DBReport, DBRow...) method}:
 * just provide the report and any additional conditions required as DBRow
 * examples. Conditions on the examples will be added directly to the internal
 * query if the DBRow class is included in the DBReport.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBReport extends RowDefinition {

	private static final long serialVersionUID = 1L;

	private SortProvider[] sortColumns = new SortProvider[]{};
	private boolean blankQueryAllowed = false;

	/**
	 * Gets all the report rows of the supplied DBReport using only conditions
	 * supplied within the supplied DBReport.
	 *
	 * <p>
	 * Use this method to retrieve all rows when the criteria have been supplied
	 * as part of the DBReport subclass.
	 *
	 * <p>
	 * If you require extra criteria to be add to the DBReport, limiting the
	 * results to a subset, use the
	 * {@link DBReport#getAllRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) getRows method}.
	 *
	 * @param <A> DBReport type
	 * @param database database
	 * @param exampleReport exampleReport
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public static <A extends DBReport> List<A> getAllRows(DBDatabase database, A exampleReport) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return getAllRows(database, exampleReport, new DBRow[]{});
	}

	/**
	 * Change the Default Setting of Disallowing Blank Queries
	 *
	 * <p>
	 * A common mistake is creating a query without supplying criteria and
	 * accidently retrieving a huge number of rows.
	 *
	 * <p>
	 * DBvolution detects this situation and, by default, throws a
	 * {@link nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException AccidentalBlankQueryException}
	 * when it happens.
	 *
	 * <p>
	 * To change this behaviour, and allow blank queries, call
	 * {@code setBlankQueriesAllowed(true)}.
	 *
	 * @param allow - TRUE to allow blank queries, FALSE to return it to the
	 * default setting.
	 */
	public void setBlankQueryAllowed(boolean allow) {
		this.blankQueryAllowed = allow;
	}

	/**
	 * Reports whether or not this DBReport is allowed to return all rows without
	 * restriction.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if blank queries are allowed, otherwise FALSE
	 */
	public boolean getBlankQueryAllowed() {
		return this.blankQueryAllowed;
	}

	/**
	 * Gets all the report rows of the supplied DBReport using conditions in the
	 * DBreport and the supplied examples.
	 *
	 * <p>
	 * Use this method to retrieve all rows when the criteria have been supplied
	 * as part of the DBReport subclass.
	 *
	 * @param <A> DBReport type
	 * @param database database
	 * @param exampleReport exampleReport
	 * @param extraExamples
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public static <A extends DBReport> List<A> getAllRows(DBDatabase database, A exampleReport, DBRow... extraExamples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(database, exampleReport, extraExamples);
		List<A> reportRows;
		query.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();
		reportRows = getReportsFromQueryResults(allRows, exampleReport);
		return reportRows;
	}
	private final List<DBRow> optionalTables = new ArrayList<DBRow>();

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		Field[] fields = this.getClass().getFields();
		for (Field field : fields) {
			field.setAccessible(true);
			final Object value;
			try {
				value = field.get(this);
				if (value != null && DBRow.class.isAssignableFrom(value.getClass())) {
					if (value instanceof DBRow) {
						final DBRow dbRow = (DBRow) value;
						str.append(dbRow.toString());
					}
				} else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
					if ((value instanceof QueryableDatatype)) {
						QueryableDatatype<?> qdt = (QueryableDatatype) value;
						str.append(field.getName()).append(": ").append(qdt.toString()).append(" ");
					}
				}
			} catch (IllegalArgumentException | IllegalAccessException ex) {
				throw new UnableToAccessDBReportFieldException(this, field, ex);
			}
		}
		return str.toString();
	}

	/**
	 * Gets all the report rows of the supplied DBReport limited by the supplied
	 * example rows.
	 *
	 * <p>
	 * All supplied rows should be from a DBRow subclass that is included in the
	 * report.
	 *
	 * <p>
	 * Builtin report limitation will be used, the example rows supply further
	 * details for constraining the report.
	 *
	 * <p>
	 * This method allows you to create generic reports and apply dynamic
	 * limitations such as date ranges, department name, and other highly variable
	 * parameters.
	 *
	 * @param <A> DBReport type
	 * @param database database
	 * @param exampleReport exampleReport
	 * @param rows rows
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public static <A extends DBReport> List<A> getRows(DBDatabase database, A exampleReport, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(database, exampleReport, rows);
		query.setBlankQueryAllowed(exampleReport.getBlankQueryAllowed());
		List<A> reportRows;
		List<DBQueryRow> allRows = query.getAllRows();
		reportRows = getReportsFromQueryResults(allRows, exampleReport);
		return reportRows;
	}

	/**
	 * Gets all the report rows of the supplied DBReport limited by the supplied
	 * example rows but reduce the result to only those that match the conditions.
	 *
	 * <p>
	 * All conditions should only reference the fields/column of the DBReport.
	 *
	 * <p>
	 * All supplied rows should be from a DBRow subclass that is included in the
	 * report.
	 *
	 * <p>
	 * Builtin report limitation will be used, the example rows supply further
	 * details for constraining the report.
	 *
	 * <p>
	 * This method allows you to create generic reports and apply dynamic
	 * limitations such as date ranges, department name, and other highly variable
	 * parameters.
	 *
	 * @param <A> DBReport type
	 * @param database database
	 * @param exampleReport exampleReport
	 * @param rows rows example rows that provide extra criteria
	 * @param conditions extra conditions that will be supplied to the WHERE or
	 * HAVING clause of the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBReport instances representing the results of the report
	 * query
	 * @throws java.sql.SQLException Database exceptions may be thrown
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public static <A extends DBReport> List<A> getRowsHaving(DBDatabase database, A exampleReport, DBRow[] rows, BooleanExpression... conditions) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(database, exampleReport, rows);
		List<A> reportRows;
		List<DBQueryRow> allRows = query.addConditions(conditions).getAllRows();
		reportRows = getReportsFromQueryResults(allRows, exampleReport);
		return reportRows;
	}

	private static <A extends DBReport> List<A> getReportsFromQueryResults(List<DBQueryRow> allRows, A exampleReport) {
		List<A> reportRows = new ArrayList<A>();
		for (DBQueryRow row : allRows) {
			reportRows.add(DBReport.getReportInstance(exampleReport, row));
		}
		return reportRows;
	}

	/**
	 * Generates and returns the actual SQL to be used by this DBReport.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Generates the SQL query for retrieving the objects but does not execute the
	 * SQL. Use
	 * {@link #getAllRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport)  the getAllRows method}
	 * to retrieve the rows.
	 *
	 * <p>
	 * See also
	 * {@link #getSQLForCount(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...)}
	 *
	 * @param <A> the class of the supplied report.
	 * @param database the database the SQL will be run against.
	 * @param exampleReport the report required.
	 * @param rows additional conditions to apply to the report.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL that will be used by this DBQuery. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public static <A extends DBReport> String getSQLForQuery(DBDatabase database, A exampleReport, DBRow... rows) throws SQLException {
		DBQuery query = getDBQuery(database, exampleReport, rows);
		return query.getSQLForQuery();
	}

	/**
	 * Returns the SQL query that will used to count the rows returned for the
	 * supplied DBReport
	 *
	 * <p>
	 * Use this method to check the SQL that will be executed during
	 * {@link DBReport#count(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...)  the count method}
	 *
	 * @param database the database to format the query for.
	 * @param exampleReport the report to retrieve.
	 * @param rows additional conditions to be applied.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this report 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public static String getSQLForCount(DBDatabase database, DBReport exampleReport, DBRow... rows) throws SQLException {
		DBQuery query = getDBQuery(database, exampleReport, rows);
		return query.getSQLForCount();
	}

	/**
	 * Count the rows on the database without retrieving the rows.
	 *
	 * <p>
	 * Creates a
	 * {@link #getSQLForCount(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...)  count query}
	 * for the report and conditions and retrieves the number of rows that would
	 * have been returned had
	 * {@link #getAllRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBReport)  getAllRows method}
	 * been called.
	 *
	 * @param database the database to format the query for.
	 * @param exampleReport the report required.
	 * @param rows additional conditions for the query.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the number of rows that have or will be retrieved. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public static Long count(DBDatabase database, DBReport exampleReport, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery setUpQuery = getDBQuery(database, exampleReport, rows);
		return setUpQuery.count();
	}

	/**
	 * Sets the sort order of DBReport (field and/or method) by the given column
	 * providers.
	 *
	 * <p>
	 * For example the following code snippet will sort by just the name column:
	 * <pre>
	 * CustomerReport customers = ...;
	 * customers.setSortOrder(customers.column(customers.name));
	 * </pre>
	 *
	 * @param columns a list of columns to sort the query by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBReport instance
	 */
	public DBReport setSortOrder(SortProvider... columns) {
		sortColumns = new SortProvider[columns.length];
		System.arraycopy(columns, 0, getSortColumns(), 0, columns.length);
		return this;
	}

	/**
	 * Sets the sort order of DBReport (field and/or method) by the given column
	 * providers.
	 *
	 * <p>
	 * For example the following code snippet will sort by just the name column:
	 * <pre>
	 * CustomerReport customers = ...;
	 * customers.setSortOrder(customers.column(customers.name));
	 * </pre>
	 *
	 * @param columns a list of columns to sort the query by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBReport instance
	 */
	public DBReport setSortOrder(ColumnProvider... columns) {
		ArrayList<SortProvider> sorters = new ArrayList<SortProvider>(0);
		for (ColumnProvider column : columns) {
			sorters.add(column.getSortProvider());
		}
		return this.setSortOrder(sorters.toArray(new SortProvider[]{}));
	}

	/**
	 * Sets the sort order of DBReport (field and/or method) by the given column
	 * providers.
	 *
	 * <p>
	 * ONLY USE FIELDS FROM THE SAME INSTANCE.
	 * <p>
	 * For example the following code snippet will sort by the name and
	 * accountNumber columns:
	 * <pre>
	 * CustomerReport customers = ...;
	 * customers.setSortOrder(customers.name, customers.accountNumber);
	 * </pre>
	 *
	 * @param columns a list of columns to sort the query by.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBReport instance
	 */
	public DBReport setSortOrder(QueryableDatatype<?>... columns) {
		List<SortProvider> sorters = new ArrayList<>();
		for (QueryableDatatype<?> qdt : columns) {
			final SortProvider expr = this.column(qdt).getSortProvider();
			sorters.add(expr);
		}
		return this.setSortOrder(sorters.toArray(new SortProvider[]{}));
	}

	/**
	 * Add the rows as optional tables in the query.
	 *
	 * @param examples
	 */
	public void addAsOptionalTables(DBRow... examples) {
		optionalTables.addAll(Arrays.asList(examples));
	}

	static <A extends DBReport> DBQuery getDBQuery(DBDatabase database, A exampleReport, DBRow... rows) {
		DBQuery query = database.getDBQuery();
		exampleReport.addTablesAndExpressions(query, exampleReport);
		query.addExtraExamples(rows);
		query.setSortOrder(exampleReport.getSortColumns());
		return query;
	}

	<A extends DBReport> void addTablesAndExpressions(DBQuery query, A exampleReport) {
		Field[] fields = exampleReport.getClass().getDeclaredFields();
		if (fields.length == 0) {
			throw new UnableToAccessDBReportFieldException(exampleReport, null);
		}
		for (Field field : fields) {
			field.setAccessible(true);
			final Object value;
			try {
				value = field.get(exampleReport);
				if (value != null && DBRow.class.isAssignableFrom(value.getClass())) {
					if (value instanceof DBRow) {
						final DBRow dbRow = (DBRow) value;
						dbRow.removeAllFieldsFromResults();
						if (optionalTables.contains(dbRow)) {
							query.addOptional(dbRow);
						} else {
							query.add(dbRow);
						}
					}
				} else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
					final QueryableDatatype<?> qdtValue = (QueryableDatatype) value;
					if ((value instanceof QueryableDatatype) && qdtValue.hasColumnExpression()) {
						query.addExpressionColumn(value, qdtValue);
						final DBExpression[] columnExpressions = qdtValue.getColumnExpression();
						for (DBExpression columnExpression : columnExpressions) {
							if (!columnExpression.isAggregator()) {
								query.addGroupByColumn(value, columnExpression);
							}
						}
					}
				}
			} catch (IllegalArgumentException | IllegalAccessException ex) {
				throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static <A extends DBReport> A getReportInstance(A exampleReport, DBQueryRow row) {
		try {
			A newReport = (A) exampleReport.getClass().newInstance();
			Field[] fields = exampleReport.getClass().getDeclaredFields();
			for (Field field : fields) {
				field.setAccessible(true);
				final Object exampleFieldValue;
				try {
					exampleFieldValue = field.get(exampleReport);
					if (exampleFieldValue != null && DBRow.class.isAssignableFrom(exampleFieldValue.getClass())) {
						if (exampleFieldValue instanceof DBRow) {
							DBRow gotDefinedRow = row.get((DBRow) exampleFieldValue);
							field.set(newReport, gotDefinedRow);
						}
					} else if (exampleFieldValue != null && QueryableDatatype.class.isAssignableFrom(exampleFieldValue.getClass())) {
						final QueryableDatatype<?> qdt = (QueryableDatatype) exampleFieldValue;
						if ((exampleFieldValue instanceof QueryableDatatype) && qdt.hasColumnExpression()) {
							final QueryableDatatype<?> expressionColumnValue = row.getExpressionColumnValue(qdt);
							field.set(newReport, expressionColumnValue);
						}
					}
				} catch (IllegalArgumentException ex) {
					throw new UnableToSetDBReportFieldException(exampleReport, field, ex);
				} catch (IllegalAccessException ex) {
					throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
				}
			}
			return newReport;
		} catch (InstantiationException | IllegalAccessException ex) {
			throw new UnableToInstantiateDBReportSubclassException(exampleReport, ex);
		}
	}

	/**
	 * Returns the list of sort columns
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the sortColumns
	 */
	protected SortProvider[] getSortColumns() {
		return sortColumns;
	}
}
