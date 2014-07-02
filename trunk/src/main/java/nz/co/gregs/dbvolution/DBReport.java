/*
 * Copyright 2014 gregory.graham.
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

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.UnableToAccessDBReportFieldException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBReportSubclassException;
import nz.co.gregs.dbvolution.expressions.DBExpression;
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
 * @author gregory.graham
 */
public class DBReport extends RowDefinition {

	private static final long serialVersionUID = 1L;

	protected ColumnProvider[] sortColumns = new ColumnProvider[]{};

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
	 * {@link DBReport#getRows(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) getRows method}.
	 *
	 * @param <A>
	 * @param database
	 * @param exampleReport
	 * @return a list of DBReport instances representing the results of the report
	 * query.
	 * @throws SQLException
	 */
	public static <A extends DBReport> List<A> getAllRows(DBDatabase database, A exampleReport) throws SQLException {
		DBQuery query = setUpQuery(database, exampleReport, new DBRow[]{});
		List<A> reportRows = new ArrayList<A>();
		query.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();
		for (DBQueryRow row : allRows) {
			reportRows.add(DBReport.getReportInstance(exampleReport, row));
		}
		return reportRows;
	}

	/**
	 * Gets all the report rows of the supplied DBReport using the supplied
	 * example rows.
	 *
	 * All supplied rows should be from a DBRow subclass that is included in the
	 * report.
	 *
	 * @param <A>
	 * @param database
	 * @param exampleReport
	 * @param rows
	 * @return a list of DBReport instances representing the results of the report
	 * query.
	 * @throws SQLException
	 */
	public static <A extends DBReport> List<A> getRows(DBDatabase database, A exampleReport, DBRow... rows) throws SQLException {
		DBQuery query = setUpQuery(database, exampleReport, rows);
		List<A> reportRows = new ArrayList<A>();
		List<DBQueryRow> allRows = query.getAllRows();
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
	 * {@link #getAllRows(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport)  the getAllRows method}
	 * to retrieve the rows.
	 *
	 * <p>
	 * See also
	 * {@link #getSQLForCount(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...)}
	 *
	 * @param <A> the class of the supplied report.
	 * @param database the database the SQL will be run against.
	 * @param exampleReport the report required.
	 * @param rows additional conditions to apply to the report.
	 * @return a String of the SQL that will be used by this DBQuery.
	 * @throws SQLException
	 */
	public static <A extends DBReport> String getSQLForQuery(DBDatabase database, A exampleReport, DBRow... rows) throws SQLException {
		DBQuery query = setUpQuery(database, exampleReport, rows);
		return query.getSQLForQuery();
	}

	/**
	 * Returns the SQL query that will used to count the rows returned for the
	 * supplied DBReport
	 *
	 * <p>
	 * Use this method to check the SQL that will be executed during
	 * {@link DBReport#count(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...)  the count method}
	 *
	 * @param database the database to format the query for.
	 * @param exampleReport the report to retrieve.
	 * @param rows additional conditions to be applied.
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this report
	 * @throws SQLException
	 */
	public static String getSQLForCount(DBDatabase database, DBReport exampleReport, DBRow... rows) throws SQLException {
		DBQuery query = setUpQuery(database, exampleReport, rows);
		return query.getSQLForCount();
	}

	/**
	 * Count the rows on the database without retrieving the rows.
	 *
	 * <p>
	 * Creates a
	 * {@link #getSQLForCount(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...)  count query}
	 * for the report and conditions and retrieves the number of rows that would
	 * have been returned had
	 * {@link #getAllRows(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport)  getAllRows method}
	 * been called.
	 *
	 * @param database the database to format the query for.
	 * @param exampleReport the report required.
	 * @param rows additional conditions for the query.
	 * @return the number of rows that have or will be retrieved.
	 * @throws SQLException
	 */
	public static Long count(DBDatabase database, DBReport exampleReport, DBRow... rows) throws SQLException {
		DBQuery setUpQuery = setUpQuery(database, exampleReport, rows);
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
	 * @return this DBReport instance
	 */
	public DBReport setSortOrder(ColumnProvider... columns) {
		sortColumns = new ColumnProvider[columns.length];
		System.arraycopy(columns, 0, sortColumns, 0, columns.length);
		return this;
	}

	/**
	 * Sets the sort order of DBReport (field and/or method) by the given column
	 * providers.
	 *
	 * <p>
	 * ONLY USE FIELDS FROM THE SAME INSTANCE.
	 * <p>
	 * For example the following code snippet will sort by just the name column:
	 * <pre>
	 * CustomerReport customers = ...;
	 * customers.setSortOrder(customers.name, customers.accountNumber);
	 * </pre>
	 *
	 * @param columns a list of columns to sort the query by.
	 * @return this DBReport instance
	 */
	public DBReport setSortOrder(QueryableDatatype... columns) {
		List<ColumnProvider> columnProviders = new ArrayList<ColumnProvider>();
		for (QueryableDatatype qdt : columns) {
			final DBExpression expr = this.column(qdt);
			if (expr instanceof ColumnProvider) {
				columnProviders.add((ColumnProvider)expr);
			}
		}
		sortColumns = columnProviders.toArray(new ColumnProvider[]{});
		return this;
	}

	private static <A extends DBReport> DBQuery setUpQuery(DBDatabase database, A exampleReport, DBRow[] rows) {
		DBQuery query = database.getDBQuery();
		addTablesAndExpressions(query, exampleReport);
		query.addExtraExamples(rows);
		query.setSortOrder(exampleReport.sortColumns);
		return query;
	}

	private static <A extends DBReport> void addTablesAndExpressions(DBQuery query, A exampleReport) {
		Field[] fields = exampleReport.getClass().getFields();
		if (fields.length == 0) {
			throw new UnableToAccessDBReportFieldException(exampleReport, null, null);
		}
		for (Field field : fields) {
			final Object value;
			try {
				value = field.get(exampleReport);
				if (value != null && DBRow.class.isAssignableFrom(value.getClass())) {
					if (value instanceof DBRow) {
						final DBRow dbRow = (DBRow) value;
						dbRow.removeAllFieldsFromResults();
						query.add(dbRow);
					}
				} else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
					if ((value instanceof QueryableDatatype) && ((QueryableDatatype) value).hasColumnExpression()) {
						final DBExpression columnExpression = ((QueryableDatatype) value).getColumnExpression();
						query.addExpressionColumn(value, columnExpression);
						if (!columnExpression.isAggregator()) {
							query.addGroupByColumn(value, columnExpression);
						}
					}
				}
			} catch (IllegalArgumentException ex) {
				throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
			} catch (IllegalAccessException ex) {
				throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static <A extends DBReport> A getReportInstance(A exampleReport, DBQueryRow row) {
		try {
			A newReport = (A) exampleReport.getClass().newInstance();
			Field[] fields = exampleReport.getClass().getFields();
			for (Field field : fields) {
				final Object value;
				try {
					value = field.get(exampleReport);
					if (value != null && DBRow.class.isAssignableFrom(value.getClass())) {
						if (value instanceof DBRow) {
							DBRow gotDefinedRow = row.get((DBRow) value);
							field.set(newReport, gotDefinedRow);
						}
					} else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
						if ((value instanceof QueryableDatatype) && ((QueryableDatatype) value).hasColumnExpression()) {
							field.set(newReport, row.getExpressionColumnValue(value));
						}
					}
				} catch (IllegalArgumentException ex) {
					throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
				} catch (IllegalAccessException ex) {
					throw new UnableToAccessDBReportFieldException(exampleReport, field, ex);
				}
			}
			return newReport;
		} catch (InstantiationException ex) {
			throw new UnableToInstantiateDBReportSubclassException(exampleReport, ex);
		} catch (IllegalAccessException ex) {
			throw new UnableToInstantiateDBReportSubclassException(exampleReport, ex);
		}
	}
}
