/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.internal.query.QueryDetails;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.actions.DBQueryInsertAction;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.query.*;

/**
 * DBQueryInsert performs a SELECT...INTO query of data from one table to
 * another.
 *
 * <p>
 * DBQueryInsert allows you to create a query that produces rows of another
 * table/DBRow.</p>
 *
 * <p>
 * Additionally the rows that can be either returned like a normal DBTable style
 * query or inserted directly into the target table.</p>
 *
 * <p>
 * The functionality is analogous to the standard SQL SELECT... INTO... and the
 * INSERT ... SELECT patterns.</p>
 *
 * <p>
 * The easiest way to create a DBQueryInsert is using {@link DBDatabase#getDBQueryInsert(nz.co.gregs.dbvolution.DBRow)
 * }</p>
 *
 * <p>
 * a DBQueryInsert requires a subclass of the DBRow to work. That is called the
 * migration target and needs to be extended to produce a migration mapper.</p>
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
 * <p>
 * For instance to map the integer A field and the string B field of the AB
 * table to the single string C field of the CD table, you should use</p>
 *
 * <code>
 * <br>
 * public class AB extends DBRow{<br> {@literal @}DBColumn DBInteger a = new
 * DBInteger();<br> {@literal @}DBColumn DBString b = new DBString();<br>
 * }<br>
 * <br>
 * public class CD extends DBRow{<br> {@literal @}DBColumn DBString c = new
 * DBString();<br>
 * }<br>
 * <br>
 * public class ABCDMapping extends CD{<br>
 * public AB ab = new AB();<br>
 * <br>
 * {<br>
 * c = new DBString(ab.column(ab.a).append(ab.column(ab.b)));<br>
 * }<br>
 * <br>
 * DBQueryInsert&lt;?&gt; migration = dbDatabase.getDBQueryInsert(ABCDMapping);
 * </code>
 *
 * <p>
 * Retrieve all the rows in the migrated form using {@link #getAllRows() }:</p>
 * <code>
 * migration.getAllRows();
 * </code>
 *
 * <p>
 * Migrate all the rows from one table to the other (does not delete anything)
 * with {@link #insertAllRows(nz.co.gregs.dbvolution.DBRow[]) }:</p>
 * <code>
 * migration.insertAllRows();
 * </code>
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 * @param <M>
 */
public class DBQueryInsert<M extends DBRow> extends RowDefinition {

	private final DBDatabase database;
	private final M mapper;
	private final List<DBRow> optionalTables = new ArrayList<>();

	public DBQueryInsert(DBDatabase db, M migrationMapper) {
		this.database = db;
		this.mapper = migrationMapper;
	}

	/**
	 * Gets all the migrated rows using only conditions supplied within the
	 * supplied DBReport.
	 *
	 * <p>
	 * Use this method to retrieve all rows when the criteria have been supplied
	 * as part of the DBQueryInsert subclass.
	 *
	 * <p>
	 * If you require extra criteria to be add to the DBQueryInsert, limiting the
	 * results to a subset, use the
	 * {@link #getAllRows(nz.co.gregs.dbvolution.DBRow...) other getAllRows method}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of DBReport instances representing the results of the report
	 * query. Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<M> getAllRows() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return getAllRows(database);
	}

	private DBQueryInsert<M> addTablesAndExpressions(DBQuery query) {
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
					final QueryableDatatype<?> qdtValue = (QueryableDatatype) value;
					if ((value instanceof QueryableDatatype) && qdtValue.hasColumnExpression()) {
						query.addExpressionColumn(value, qdtValue);
						final DBExpression[] columnExpressions = qdtValue.getColumnExpression();
						for (DBExpression columnExpression : columnExpressions) {
//							query.addExpressionColumn(value, columnExpression);
							if (!columnExpression.isAggregator()) {
								query.addGroupByColumn(value, columnExpression);
							}
						}
					}
				}
			} catch (IllegalArgumentException | IllegalAccessException ex) {
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

	private M getMappedTarget(DBQueryRow row) {
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
		} catch (InstantiationException | IllegalAccessException ex) {
			throw new UnableToInstantiateDBMigrationSubclassException(this, ex);
		}
	}

	private static final long serialVersionUID = 1L;

	private SortProvider[] sortColumns = new SortProvider[]{};
	Boolean cartesian = false;
	Boolean blank = false;

	/**
	 * Gets all the migrated rows using conditions in the DBQueryInsert and the
	 * supplied examples.
	 *
	 * @param extraExamples extra rows defining additional criteria
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<M> getAllRows(DBRow... extraExamples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		return getAllRows(database, extraExamples);
	}

	private List<M> getAllRows(DBDatabase database, DBRow... extraExamples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(database, extraExamples);
//		query.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();
		List<M> reportRows = getInsertedRowsFromQueryResults(allRows);
		return reportRows;
	}

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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<M> getRows(DBDatabase database, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(database, rows);
		List<DBQueryRow> allRows = query.getAllRows();
		List<M> reportRows = getInsertedRowsFromQueryResults(allRows);
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
	 * Built-in report limitation will be used, the example rows supply further
	 * details for constraining the report.
	 *
	 * <p>
	 * This method allows you to create generic reports and apply dynamic
	 * limitations such as date ranges, department name, and other highly variable
	 * parameters.
	 *
	 * @param database database
	 * @param rows rows example rows that provide extra criteria
	 * @param conditions the conditions that will be supplied to the WHERE or
	 * HAVING clause of the query
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of DBReport instances representing the results of the report
	 * query
	 * @throws java.sql.SQLException Database exceptions may be thrown
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public List<M> getRowsHaving(DBDatabase database, DBRow[] rows, BooleanExpression... conditions) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(database, rows);
		List<M> reportRows;
		List<DBQueryRow> allRows = query.addConditions(conditions).getAllRows();
		reportRows = getInsertedRowsFromQueryResults(allRows);
		return reportRows;
	}

	private List<M> getInsertedRowsFromQueryResults(List<DBQueryRow> allRows) {
		List<M> reportRows = new ArrayList<>();
		for (DBQueryRow row : allRows) {
			reportRows.add(getMappedTarget(row));
		}
		return reportRows;
	}

	/**
	 * Generates and returns the actual SQL to be used by this DBQueryInsert to
	 * select the rows to insert.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Generates the SQL query for retrieving the objects but does not execute the
	 * SQL. Use
	 * {@link #getAllRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBRow...) the getAllRows method}
	 * to retrieve the rows.
	 *
	 * <p>
	 * See also
	 * {@link #getSQLForCount(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBRow...) }
	 *
	 * @param database the database the SQL will be run against.
	 * @param rows additional conditions to apply to the report.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL that will be used by this DBQuery. 1 Database
	 * exceptions may be thrown
	 */
	public String getSQLForQuery(DBDatabase database, DBRow... rows) {
		DBQuery query = getDBQuery(database, rows);
		return query.getSQLForQuery();
	}

	/**
	 * Generates and returns the actual SQL to be used by this DBQueryInsert to
	 * insert the queried rows.
	 *
	 * <p>
	 * Good for debugging and great for DBAs, this is how you find out what
	 * DBvolution is really doing.
	 *
	 * <p>
	 * Generates the SQL query for retrieving the objects but does not execute the
	 * SQL. Use
	 * {@link #getAllRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBRow...) the getAllRows method}
	 * to retrieve the rows.
	 *
	 * <p>
	 * See also
	 * {@link #getSQLForCount(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBRow...) }
	 *
	 * @param database the database the SQL will be run against.
	 * @param rows additional conditions to apply to the report.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the SQL that will be used by this DBQuery. 1 Database
	 * exceptions may be thrown
	 */
	public String getSQLForInsert(DBDatabase database, DBRow... rows) {
		DBQueryInsertAction<M> action = getDBQueryInsertAction(rows);
		ArrayList<String> sqlStatements = action.getSQLStatements(database);
		if (sqlStatements.size() > 0) {
			return sqlStatements.get(0);
		} else {
			return "";
		}
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
	 * @param rows additional conditions to be applied.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * {@link #getSQLForCount(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBRow...)  count query}
	 * for the report and conditions and retrieves the number of rows that would
	 * have been returned had
	 * {@link #getAllRows(nz.co.gregs.dbvolution.databases.DBDatabase, nz.co.gregs.dbvolution.DBRow...) getAllRows method}
	 * been called.
	 *
	 * @param database the database to format the query for.
	 * @param rows additional conditions for the query.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the number of rows that have or will be retrieved. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public Long count(DBDatabase database, DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBReport instance
	 */
	public DBQueryInsert<M> setSortOrder(SortProvider... columns) {
		sortColumns = new SortProvider[columns.length];
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBReport instance
	 */
	public DBQueryInsert<M> setSortOrder(QueryableDatatype<?>... columns) {
		List<SortProvider> columnProviders = new ArrayList<>();
		for (QueryableDatatype<?> qdt : columns) {
			final ColumnProvider expr = this.column(qdt);
			columnProviders.add(expr.getSortProvider());
		}
		sortColumns = columnProviders.toArray(new SortProvider[]{});
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the sortColumns
	 */
	protected SortProvider[] getSortColumns() {
		return sortColumns;
	}

	/**
	 * Suppresses Cartesian join error protection.
	 *
	 * <p>
	 * DBvolution protects you from accidental Cartesian joins but use this
	 * function if a Cartesian is required.</p>
	 *
	 * <p>
	 * Cartesian joins occur when there is no connection between 2 (or more)
	 * tables. Normally all tables are connect by a chain of relationships,
	 * usually primary key to foreign key.</p>
	 * <p>
	 * Sometimes a connection is missed: for instance 2 unrelated tables are being
	 * compared by price, but the price relating expression has not been added. In
	 * this case DBvolution will throw an {@link AccidentalCartesianJoinException}
	 * and abort the query. This exception avoids creating a probably massive
	 * dataset that will reduce database and network performance
	 * significantly.</p>
	 * <p>
	 * However there are valid cases for a Cartesian join: finding all possible
	 * combinations of cake and coffee for instance.</p>
	 * <p>
	 * If you are sure you need a Cartesian join, use this method to avoid the
	 * error-checking and the {@link AccidentalCartesianJoinException}</p>
	 *
	 * @param setting True if you need a Cartesian join in this DBQueryInsert.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQueryInsert object
	 */
	public DBQueryInsert<M> setCartesianJoinAllowed(Boolean setting) {
		cartesian = setting;
		return this;
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
	 * @param setting - TRUE to allow blank queries, FALSE to return it to the
	 * default setting.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this DBQueryInsert instance
	 */
	public DBQueryInsert<M> setBlankQueryAllowed(Boolean setting) {
		blank = setting;
		return this;
	}

	/**
	 * Perform the full migration defined by this DBQueryInsert.
	 *
	 * <p>
	 * DBQueryInsert allows you to create a query that produces rows of another
	 * table/DBRow.</p>
	 *
	 * <p>
	 * Additionally the rows that can be either returned like a normal DBTable
	 * style query or inserted directly into the target table.</p>
	 *
	 * <p>
	 * The functionality is analogous to the standard SQL INSERT... INTO... FROM
	 * pattern.</p>
	 *
	 * <p>
	 * The easiest way to create a DBQueryInsert is using {@link DBDatabase#getDBQueryInsert(nz.co.gregs.dbvolution.DBRow)
	 * }</p>
	 *
	 * <p>
	 * a DBQueryInsert requires a subclass of the DBRow to work. That is called
	 * the migration target and needs to be extend to produce a migration
	 * mapper.</p>
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
	 * Field mappings are also added into the initialization block, by replacing
	 * the target table's field values with column expressions that map the
	 * columns of the source tables to the fields off the target table.</p>
	 *
	 * <p>
	 * For instance to map the integer A field and the string B field of the AB
	 * table to the single string C field of the CD table, you should use</p>
	 *
	 * <code>
	 * <br>
	 * public class AB extends DBRow{<br> {@literal @}DBColumn DBInteger a = new
	 * DBInteger();<br> {@literal @}DBColumn DBString b = new DBString();<br>
	 * }<br>
	 * <br>
	 * public class CD extends DBRow{<br> {@literal @}DBColumn DBString c = new
	 * DBString();<br>
	 * }<br>
	 * <br>
	 * public class ABCDMapping extends CD{<br>
	 * public AB ab = new AB();<br>
	 * <br>
	 * {<br>
	 * c = new DBString(ab.column(ab.a).append(ab.column(ab.b)));<br>
	 * }<br>
	 * <br>
	 * DBQueryInsert&lt;?&gt; migration =
	 * dbDatabase.getDBQueryInsert(ABCDMapping);
	 * </code>
	 *
	 * <p>
	 * Retrieve all the rows in the migrated form using {@link #getAllRows()
	 * }:</p>
	 * <code>
	 * migration.getAllRows();
	 * </code>
	 *
	 * <p>
	 * Migrate all the rows from one table to the other (does not delete anything)
	 * with {@link #insertAllRows(nz.co.gregs.dbvolution.DBRow[]) }:</p>
	 * <code>
	 * migration.insertAllRows();
	 * </code>
	 *
	 * @param extraExamples
	 * @throws SQLException
	 */
	public void insertAllRows(DBRow... extraExamples) throws SQLException {
		DBQueryInsertAction<M> migrate = getDBQueryInsertAction(extraExamples);
		migrate.migrate(database);
	}

	private DBQueryInsertAction<M> getDBQueryInsertAction(DBRow[] extraExamples) {
		return new DBQueryInsertAction<>(this, this.mapper, extraExamples);
	}

	/**
	 * Validate the migration defined by this DBQueryInsert but do not make
	 * perform any actual inserts.
	 *
	 * <p>
	 * DBQueryInsert allows you to create a query that produces rows of another
	 * table/DBRow.</p>
	 *
	 * <p>
	 * Additionally the rows that can be either returned like a normal DBTable
	 * style query or inserted directly into the target table.</p>
	 *
	 * <p>
	 * The functionality is analogous to the standard SQL INSERT... INTO... FROM
	 * pattern.</p>
	 *
	 * <p>
	 * The easiest way to create a DBQueryInsert is using {@link DBDatabase#getDBQueryInsert(nz.co.gregs.dbvolution.DBRow)
	 * }</p>
	 *
	 * <p>
	 * a DBQueryInsert requires a subclass of the DBRow to work. That is called
	 * the migration target and needs to be extend to produce a migration
	 * mapper.</p>
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
	 * Field mappings are also added into the initialization block, by replacing
	 * the target table's field values with column expressions that map the
	 * columns of the source tables to the fields off the target table.</p>
	 *
	 * <p>
	 * For instance to map the integer A field and the string B field of the AB
	 * table to the single string C field of the CD table, you should use</p>
	 *
	 * <code>
	 * <br>
	 * public class AB extends DBRow{<br> {@literal @}DBColumn DBInteger a = new
	 * DBInteger();<br> {@literal @}DBColumn DBString b = new DBString();<br>
	 * }<br>
	 * <br>
	 * public class CD extends DBRow{<br> {@literal @}DBColumn DBString c = new
	 * DBString();<br>
	 * }<br>
	 * <br>
	 * public class ABCDMapping extends CD{<br>
	 * public AB ab = new AB();<br>
	 * <br>
	 * {<br>
	 * c = new DBString(ab.column(ab.a).append(ab.column(ab.b)));<br>
	 * }<br>
	 * <br>
	 * DBQueryInsert&lt;?&gt; migration =
	 * dbDatabase.getDBQueryInsert(ABCDMapping);
	 * </code>
	 *
	 * <p>
	 * Retrieve all the rows in the migrated form using {@link #getAllRows()
	 * }:</p>
	 * <code>
	 * migration.getAllRows();
	 * </code>
	 *
	 * <p>
	 * Migrate all the rows from one table to the other (does not delete anything)
	 * with {@link #insertAllRows(nz.co.gregs.dbvolution.DBRow[]) }:</p>
	 * <code>
	 * migration.insertAllRows();
	 * </code>
	 *
	 * @param extraExamples
	 * @throws SQLException
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the validation results of the migration
	 */
	public DBValidation.Results validateAllRows(DBRow... extraExamples) throws SQLException {

		DBValidation<M> validate = new DBValidation<>(this, this.mapper, extraExamples);
		return validate.validate(database);
	}

	QueryDetails getQueryDetails() {
		return this.getDBQuery(database).getQueryDetails();
	}

}
