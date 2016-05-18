/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.actions.DBMigrate;
import nz.co.gregs.dbvolution.actions.DBValidate;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.UnableToAccessDBMigrationFieldException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBMigrationSubclassException;
import nz.co.gregs.dbvolution.exceptions.UnableToSetDBMigrationFieldException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 * DBMigration performs a migration of data from one table to another.
 *
 * <p>
 * DBMigration allows you to create a query that produces rows of another
 * table/DBRow.</p>
 *
 * <p>
 * Additionally the rows that can be either returned like a normal DBTable style
 * query or inserted directly into the target table.</p>
 *
 * <p>
 * The functionality is analogous to the standard SQL INSERT... INTO... FROM
 * pattern.</p>
 *
 * <p>
 * The easiest way to create a DBMigration is using {@link DBDatabase#getDBMigration(nz.co.gregs.dbvolution.DBRow)
 * }</p>
 *
 * <p>
 * a DBMigration requires a subclass of the DBRow to work. That is called the
 * migration target and needs to be extend to produce a migration mapper.</p>
 *
 * <p>
 * The migration mapper is an extension of the migration target that includes
 * sources tables, source criteria, and field mappings as described below.</p>
 *
 * <p>
 * Source tables are DBRow instances added to the migration mapping as new
 * fields. These are added together in a DBQuery to produce the underlying
 * database query that the source data will come from.</p>
 *
 * <p>
 * Criteria can be added to the source tables in an initialization block and
 * will restrict the underlying query to a subset of rows</p>
 *
 * <p>
 * Field mappings are also added into the initialization block, by replacing the
 * target table's field values with column expressions that map the columns of
 * the source tables to the fields off the target table.</p>
 * 
 * <p>For instance to map the integer A field and the string B field of the AB table to the single string C field of the CD table, you should  use</p>
 * 
 * <code>
 * <br>
 * public class AB extends DBRow{<br>
 *{@literal @}DBColumn DBInteger a = new DBInteger();<br>
 * {@literal @}DBColumn DBString b = new DBString();<br>
 * }<br>
 *<br>
 * public class CD extends DBRow{<br>
 *{@literal @}DBColumn DBString c = new DBString();<br>
 * }<br>
 * <br>
 * public class ABCDMapping extends CD{<br>
 * public AB ab = new AB();<br>
 * <br>
 * {<br>
 * c = new DBString(ab.column(ab.a).append(ab.column(ab.b)));<br>
 * }<br>
 * <br>
 * dbDatabase.getDBMigration(ABCDMapping);
 *</code>
 * @author gregorygraham
 * @param <M>
 */
public class DBMigration<M extends DBRow> extends RowDefinition {

	private final DBDatabase database;
	private final M mapper;

	DBMigration(DBDatabase db, M migrationMapper) {
		this.database = db;
		this.mapper = migrationMapper;
	}

	@SuppressWarnings("unchecked")
	public List<M> getAllRows() throws SQLException {
		return getAllRows(database);
	}

	private DBMigration<M> addTablesAndExpressions(DBQuery query) {
		Field[] fields = mapper.getClass().getFields();
		if (fields.length == 0) {
			throw new UnableToAccessDBMigrationFieldException(this, null);
		}
		for (Field field : fields) {
			field.setAccessible(true);
			final Object value;
			try {
				value = field.get(mapper);
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
					if ((value instanceof QueryableDatatype) && ((QueryableDatatype) value).hasColumnExpression()) {
						final DBExpression columnExpression = ((QueryableDatatype) value).getColumnExpression();
						query.addExpressionColumn(value, columnExpression);
						if (!columnExpression.isAggregator()) {
							query.addGroupByColumn(value, columnExpression);
						}
					}
				}
			} catch (IllegalArgumentException ex) {
				throw new UnableToAccessDBMigrationFieldException(this, field, ex);
			} catch (IllegalAccessException ex) {
				throw new UnableToAccessDBMigrationFieldException(this, field, ex);
			}
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	M createInstanceOfMappingTarget() throws InstantiationException, IllegalAccessException {
		Class<? extends DBRow> aClass = mapper.getClass();
		return (M) aClass.newInstance();
	}

	M getMappedTarget(DBQueryRow row) {
		try {
			M newTarget = createInstanceOfMappingTarget();
			Field[] fields = mapper.getClass().getFields();
			for (Field field : fields) {
				field.setAccessible(true);
				final Object value;
				try {
					value = field.get(mapper);
					if (value != null && DBRow.class.isAssignableFrom(value.getClass())) {
						if (value instanceof DBRow) {
							DBRow gotDefinedRow = row.get((DBRow) value);
							try {
								Field targetField = newTarget.getClass().getField(field.getName());
								targetField.set(newTarget, gotDefinedRow);
							} catch (NoSuchFieldException ex) {
//								throw new UnableToSetDBMigrationFieldException(newTarget, field, ex);
							}
						}
					} else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
						if ((value instanceof QueryableDatatype) && ((QueryableDatatype) value).hasColumnExpression()) {
							final QueryableDatatype<?> expressionColumnValue = row.getExpressionColumnValue(value);
							try {
								Field targetField = newTarget.getClass().getField(field.getName());
								targetField.set(newTarget, expressionColumnValue);
							} catch (NoSuchFieldException ex) {
//								throw new UnableToSetDBMigrationFieldException(newTarget, field, ex);
							}
						}
					}
				} catch (IllegalArgumentException ex) {
					throw new UnableToSetDBMigrationFieldException(newTarget, field, ex);
				} catch (IllegalAccessException ex) {
					throw new UnableToAccessDBMigrationFieldException(newTarget, field, ex);
				}
			}
			return newTarget;
		} catch (InstantiationException ex) {
			throw new UnableToInstantiateDBMigrationSubclassException(this, ex);
		} catch (IllegalAccessException ex) {
			throw new UnableToInstantiateDBMigrationSubclassException(this, ex);
		}
	}

	private static final long serialVersionUID = 1L;

	private transient ColumnProvider[] sortColumns = new ColumnProvider[]{};
	Boolean cartesian = false;
	Boolean blank = false;

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
	 * {@link DBReport#getAllRows(nz.co.gregs.dbvolution.DBDatabase, nz.co.gregs.dbvolution.DBReport, nz.co.gregs.dbvolution.DBRow...) getRows method}.
	 *
	 * @param database database
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public List<M> getAllRows(DBDatabase database) throws SQLException {
		return getAllRows(database, new DBRow[]{});
	}

	/**
	 * Gets all the report rows of the supplied DBReport using conditions in the
	 * DBreport and the supplied examples.
	 *
	 * <p>
	 * Use this method to retrieve all rows when the criteria have been supplied
	 * as part of the DBReport subclass.
	 *
	 * @param database database
	 * @param extraExamples
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public List<M> getAllRows(DBDatabase database, DBRow... extraExamples) throws SQLException {
		DBQuery query = getDBQuery(database, extraExamples);
//		query.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();
		List<M> reportRows = getReportsFromQueryResults(allRows);
		return reportRows;
	}
	final List<DBRow> optionalTables = new ArrayList<>();

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
			} catch (IllegalArgumentException ex) {
				throw new UnableToAccessDBMigrationFieldException(this, field, ex);
			} catch (IllegalAccessException ex) {
				throw new UnableToAccessDBMigrationFieldException(this, field, ex);
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
	 * @param database database
	 * @param rows rows
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public List<M> getRows(DBDatabase database, DBRow... rows) throws SQLException {
		DBQuery query = getDBQuery(database, rows);
		List<DBQueryRow> allRows = query.getAllRows();
		List<M> reportRows = getReportsFromQueryResults(allRows);
		return reportRows;
	}

	/**
	 * Gets all the report rows of the supplied DBReport limited by the supplied
	 * example rows but reduce the result to only those that match the post-query
	 * conditions.
	 *
	 * <p>
	 * All post-query conditions should only reference the fields/column of the
	 * DBReport.
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
	 * @param database database
	 * @param rows rows example rows that provide extra criteria
	 * @param postQueryConditions the post-query conditions that will be supplied
	 * to the HAVING clause of the query
	 * @return a list of DBReport instances representing the results of the report
	 * query
	 * @throws java.sql.SQLException Database exceptions may be thrown
	 */
	public List<M> getRowsHaving(DBDatabase database, DBRow[] rows, BooleanExpression... postQueryConditions) throws SQLException {
		DBQuery query = getDBQuery(database, rows);
		List<M> reportRows;
		List<DBQueryRow> allRows = query.getAllRowsHaving(postQueryConditions);
		reportRows = getReportsFromQueryResults(allRows);
		return reportRows;
	}

	private List<M> getReportsFromQueryResults(List<DBQueryRow> allRows) {
		List<M> reportRows = new ArrayList<>();
		for (DBQueryRow row : allRows) {
			reportRows.add(getMappedTarget(row));
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
	 * @param database the database the SQL will be run against.
	 * @param rows additional conditions to apply to the report.
	 * @return a String of the SQL that will be used by this DBQuery. 1 Database
	 * exceptions may be thrown
	 */
	public String getSQLForQuery(DBDatabase database, DBRow... rows) {
		DBQuery query = getDBQuery(database, rows);
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
	 * @param rows additional conditions to be applied.
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this report 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public String getSQLForCount(DBDatabase database, DBRow... rows) throws SQLException {
		DBQuery query = getDBQuery(database, rows);
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
	 * @param rows additional conditions for the query.
	 * @return the number of rows that have or will be retrieved. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public Long count(DBDatabase database, DBRow... rows) throws SQLException {
		DBQuery setUpQuery = getDBQuery(database, rows);
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
	public DBMigration<M> setSortOrder(ColumnProvider... columns) {
		sortColumns = new ColumnProvider[columns.length];
		System.arraycopy(columns, 0, getSortColumns(), 0, columns.length);
		return this;
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
	 * @return this DBReport instance
	 */
	public DBMigration<M> setSortOrder(QueryableDatatype<?>... columns) {
		List<ColumnProvider> columnProviders = new ArrayList<>();
		for (QueryableDatatype<?> qdt : columns) {
			final ColumnProvider expr = this.column(qdt);
			columnProviders.add(expr);
		}
		sortColumns = columnProviders.toArray(new ColumnProvider[]{});
		return this;
	}

	/**
	 * Add the rows as optional tables in the query.
	 *
	 * @param examples
	 */
	public void addAsOptionalTables(DBRow... examples) {
		optionalTables.addAll(Arrays.asList(examples));
	}

	DBQuery getDBQuery(DBDatabase database, DBRow... rows) {
		DBQuery query = database.getDBQuery();
		query.setBlankQueryAllowed(blank);
		query.setCartesianJoinsAllowed(cartesian);
		addTablesAndExpressions(query);
		query.addExtraExamples(rows);
		query.setSortOrder(this.getSortColumns());
		return query;
	}

	/**
	 * Returns the list of sort columns
	 *
	 * @return the sortColumns
	 */
	protected ColumnProvider[] getSortColumns() {
		return sortColumns;
	}

	public DBMigration<M> setCartesianJoinAllowed(Boolean setting) {
		cartesian = setting;
		return this;
	}

	public DBMigration<M> setBlankQueryAllowed(Boolean setting) {
		blank = setting;
		return this;
	}

	public void migrateAllRows(DBRow... extraExamples) throws SQLException {
		DBMigrate<M> migrate = new DBMigrate<>(this, this.mapper, extraExamples);
		migrate.migrate(database);
	}

	public DBValidate.Results validateAllRows(DBRow... extraExamples) throws SQLException {

		DBValidate<M> validate = new DBValidate<>(this, this.mapper, extraExamples);
		return validate.validate(database);	}

}
