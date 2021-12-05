/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.internal.query.QueryDetails;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.actions.DBMigrationAction;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.query.*;

public class DBMigration<M extends DBRow> extends RowDefinition {

	private final M mapper;
	private final List<DBRow> optionalTables = new ArrayList<>();
	private final DBDatabase database;

	private DBMigration(DBDatabase database, M migrationMapper) {
		this.mapper = DBRow.copyDBRow(migrationMapper);
		this.database = database.copy();
	}

	/**
	 * Create a migration that uses the mapper provided.
	 *
	 * <p>
	 * Mapper uses a subclass of a particular DBRow to create rows for that DBRow
	 * and insert them into the table.</p>
	 *
	 * <p>
	 * Central to this concept is using other existing data in table, represented
	 * by their own DBRow classes, transformed using DBvolution expressions</p>
	 * <pre>
	 * public static class MigrateToFight extends Fight {
	 * public Villain baddy = new Villain();
	 * public Hero goody = new Hero();
	 * {
	 * baddy.name.permittedPattern("Dr%");
	 * hero = goody.column(goody.name).asExpressionColumn();
	 * villain = baddy.column(baddy.name).asExpressionColumn();
	 * }
	 * }
	 * database.createTable(new Fight());
	 * DBMigration migration = DBMigration.using(new MigrateToFight());
	 * migration.createAllRows(database);
	 * </pre>
	 *
	 * @param <MAPPER> the transformation to apply
	 * @param database the database to work with
	 * @param migrationMapper
	 * @return
	 */
	public static <MAPPER extends DBRow> DBMigration<MAPPER> using(DBDatabase database, MAPPER migrationMapper) {
		return new DBMigration<MAPPER>(database, migrationMapper);
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
				throw new UnableToAccessDBMigrationFieldException(this, field, ex);
			}
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	M createInstanceOfMappingTarget() throws InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
		Class<? extends DBRow> aClass = mapper.getClass();
		return (M) aClass.getConstructor().newInstance();
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
							}
						}
					} else if (value != null && QueryableDatatype.class.isAssignableFrom(value.getClass())) {
						if ((value instanceof QueryableDatatype) && ((QueryableDatatype) value).hasColumnExpression()) {
							final QueryableDatatype<?> expressionColumnValue = row.getExpressionColumnValue(value);
							try {
								Field targetField = newTarget.getClass().getField(field.getName());
								targetField.set(newTarget, expressionColumnValue);
							} catch (NoSuchFieldException ex) {
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
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | IllegalArgumentException | InvocationTargetException ex) {
			throw new UnableToInstantiateDBMigrationSubclassException(this, ex);
		}
	}

	private static final long serialVersionUID = 1L;

	private final ArrayList<SortProvider> sortColumns = new ArrayList<>();
	Boolean cartesian = false;
	Boolean blank = false;

	/**
	 * Gets all the migrated rows using conditions in the DBMigration and the
	 * supplied examples.
	 *
	 * @param database
	 * @param extraExamples extra rows defining additional criteria
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 * thrown if no conditions have been set and blank queries have not been
	 * explicitly allowed
	 */
	public List<M> getAllRows(DBRow... extraExamples) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(extraExamples);
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
	 * Gets all the report rows of the migration limited by the supplied example
	 * rows.
	 *
	 * <p>
	 * All supplied rows should be using a DBRow subclass that is included in the
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
	 * @param rows rows
	 * @return a list of DBReport instances representing the results of the report
	 * query. 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 * thrown if no conditions have been set and blank queries have not been
	 * explicitly allowed
	 */
	public List<M> getRows(DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(rows);
		List<DBQueryRow> allRows = query.getAllRows();
		List<M> reportRows = getInsertedRowsFromQueryResults(allRows);
		return reportRows;
	}

	/**
	 * Gets all the report rows of this DBMigration limited by the supplied
	 * example rows but reduce the result to only those that match the conditions.
	 *
	 * <p>
	 * All conditions should only reference the fields/column of the DBMigration.
	 *
	 * <p>
	 * All supplied rows should be using a DBRow subclass that is included in the
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
	 * @return a list of DBReport instances representing the results of the report
	 * query
	 * @throws java.sql.SQLException Database exceptions may be thrown
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 * thrown if no conditions have been set and blank queries have not been
	 * explicitly allowed
	 */
	public List<M> getRowsHaving(DBRow[] rows, BooleanExpression... conditions) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery query = getDBQuery(rows);
		List<M> reportRows;
		List<DBQueryRow> allRows = query.addConditions(conditions).getAllRows();
		reportRows = getInsertedRowsFromQueryResults(allRows);
		return reportRows;
	}

	private List<M> getInsertedRowsFromQueryResults(List<DBQueryRow> allRows) {
		List<M> reportRows = allRows
				.stream()
				.map((t) -> getMappedTarget(t))
				.collect(Collectors.toList());
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
	 * @param rows additional conditions to apply to the report.
	 * @return a String of the SQL that will be used by this DBQuery. 1 Database
	 * exceptions may be thrown
	 */
	public String getSQLForQuery(DBRow... rows) {
		DBQuery query = getDBQuery(rows);
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
	 * @return a String of the SQL that will be used by this DBQuery. 1 Database
	 * exceptions may be thrown
	 */
	public String getSQLForInsert(DBRow... rows) {
		DBMigrationAction<M> action = getDBMigrationAction(rows);
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
	 * @param rows additional conditions to be applied.
	 * @return a String of the SQL query that will be used to count the rows
	 * returned by this report 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public String getSQLForCount(DBRow... rows) throws SQLException {
		DBQuery query = getDBQuery(rows);
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
	 * @param rows additional conditions for the query.
	 * @return the number of rows that have or will be retrieved. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public Long count(DBRow... rows) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		DBQuery setUpQuery = getDBQuery(rows);
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
	public DBMigration<M> setSortOrder(SortProvider... columns) {
		sortColumns.clear();
		sortColumns.addAll(Arrays.asList(columns));
		return this;
	}

	/**
	 * Sets the sort order of migration (field and/or method) by the given column
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
		List<SortProvider> columnProviders = new ArrayList<>();
		for (QueryableDatatype<?> qdt : columns) {
			final ColumnProvider expr = this.column(qdt);
			columnProviders.add(expr.getSortProvider());
		}
		sortColumns.addAll(columnProviders);
		return this;
	}

	/**
	 * Add the rows as optional tables in the query.
	 *
	 * <p>
	 * Optional tables are joined to the query using an outer join and results are
	 * returned regardless of whether there is a matching row in the optional
	 * table.</p>
	 *
	 * <p>
	 * Optional rows may contain only DBNull values, that is there getValue()
	 * method will return NULL and isDBNull() will be true.</p>
	 *
	 * @param examples additional tables to include in the query if possible.
	 */
	public void addAsOptionalTables(DBRow... examples) {
		optionalTables.addAll(Arrays.asList(examples));
	}

	DBQuery getDBQuery(DBRow... rows) {
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
	protected SortProvider[] getSortColumns() {
		return sortColumns.toArray(new SortProvider[]{});
	}

	/**
	 * Suppresses Cartesian join error protection.
	 *
	 * <p>
	 * DBvolution protects you using accidental Cartesian joins but use this
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
	 * @return this DBQueryInsert object
	 */
	public DBMigration<M> setCartesianJoinAllowed(Boolean setting) {
		cartesian = setting;
		return this;
	}

	/**
	 * Change the Default Setting of Disallowing Blank Queries
	 *
	 * <p>
	 * A common mistake is creating a query without supplying criteria and
	 * accidentally retrieving a huge number of rows.
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
	 * @return this DBQueryInsert instance
	 */
	public DBMigration<M> setBlankQueryAllowed(Boolean setting) {
		blank = setting;
		return this;
	}

	public void createAllRows(DBRow... extraExamples) throws SQLException {
		DBMigrationAction<M> migrate = getDBMigrationAction(extraExamples);
		migrate.migrate(database);
	}

	private DBMigrationAction<M> getDBMigrationAction(DBRow[] extraExamples) {
		return new DBMigrationAction<>(this, mapper, extraExamples);
	}

	/**
	 * Perform a migration validation using DBMigrationValidation.
	 *
	 * <p>
	 * Validation will process all the available rows and try migrate them or to
	 * identify why they were not migrated. FAILURE TO MIGRATE MAY BE CORRECT
	 * BEHAVIOUR. Carefully check the results to determine if there are issues
	 * with the migration or the data.</p>
	 *
	 * @param extraExamples extra examples
	 * @return result from the migration validation
	 * @throws SQLException
	 */
	public DBMigrationValidation.Results validateAllRows(DBRow... extraExamples) throws SQLException {

		DBMigrationValidation<M> validate = new DBMigrationValidation<>(this, mapper, extraExamples);
		return validate.validate(database);
	}

	QueryDetails getQueryDetails() {
		return this.getDBQuery().getQueryDetails();
	}

}
