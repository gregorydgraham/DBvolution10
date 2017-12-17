/*
 * Copyright 2017 gregorygraham.
 *
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.internal.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.columns.AbstractColumn;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.columns.DateColumn;
import nz.co.gregs.dbvolution.columns.IntegerColumn;
import nz.co.gregs.dbvolution.columns.NumberColumn;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.ColumnProvidedMustBeAForeignKey;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import nz.co.gregs.dbvolution.exceptions.UnableToInterpolateReferencedColumnInMultiColumnPrimaryKeyException;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.internal.properties.RowDefinitionInstanceWrapper;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.results.StringResult;

public class RecursiveQueryDetails<T extends DBRow> extends QueryDetails {

	private DBQuery originalQuery;
	private ColumnProvider keyToFollow;
	private T typeToReturn = null;
	private RecursiveSQLDirection recursiveQueryDirection = RecursiveSQLDirection.TOWARDS_ROOT;

	/**
	 * @return the originalQuery
	 */
	public DBQuery getOriginalQuery() {
		return originalQuery;
	}

	/**
	 * @param originalQuery the originalQuery to set
	 */
	public void setOriginalQuery(DBQuery originalQuery) {
		this.originalQuery = originalQuery;
	}

	/**
	 * @return the keyToFollow
	 */
	public ColumnProvider getKeyToFollow() {
		return keyToFollow;
	}

	/**
	 * @param keyToFollow the keyToFollow to set
	 */
	public void setKeyToFollow(ColumnProvider keyToFollow) {
		this.keyToFollow = keyToFollow;
	}

	/**
	 * @return the typeToReturn
	 */
	public T getTypeToReturn() {
		return typeToReturn;
	}

	/**
	 * @param typeToReturn the typeToReturn to set
	 */
	public void setTypeToReturn(T typeToReturn) {
		this.typeToReturn = typeToReturn;
	}

	public RecursiveSQLDirection getDirection() {
		return getRecursiveQueryDirection();
	}

	/**
	 * @return the recursiveQueryDirection
	 */
	public RecursiveSQLDirection getRecursiveQueryDirection() {
		return recursiveQueryDirection;
	}

	/**
	 * @param recursiveQueryDirection the recursiveQueryDirection to set
	 */
	public void setRecursiveQueryDirection(RecursiveSQLDirection recursiveQueryDirection) {
		this.recursiveQueryDirection = recursiveQueryDirection;
	}

	@Override
	public DBActionList query(DBDatabase db) throws SQLException {
		getRowsFromRecursiveQuery(db, this);
		return new DBActionList();
	}

	/**
	 * Creates a recursive query that traverses a tree structure from the nodes
	 * provided by this query to the root of the tree.
	 *
	 * <p>
	 * Tree structures are stored in databases as a table with a foreign key to
	 * itself. This method provides a convenient way to find the path from a leaf
	 * node to the root of the tree.
	 *
	 * <p>
	 * These structures are recursive in that the table is self-referential and
	 * thus has similar properties to a recursive procedure. As such the queries
	 * used to traverse the structures are also called recursive queries.
	 *
	 * <p>
	 * This method creates a recursive query based on the current query and the
	 * foreign key provided to the traverse "down" the tree from the results of
	 * the current query thru all the rows that reference the current rows.
	 *
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A linked List
	 *
	 */
	private List<DBQueryRow> getRowsFromRecursiveQuery(DBDatabase database, RecursiveQueryDetails<T> details) throws SQLException {
		List<DBQueryRow> returnList = new ArrayList<>();
		final RecursiveSQLDirection direction = details.getDirection();
		if (database.getDefinition().supportsRecursiveQueriesNatively()) {
			returnList = performNativeRecursiveQuery(database, details, direction, returnList);
		} else {
			returnList = performRecursiveQueryEmulation(database, details, direction);
		}
		setResults(returnList);
		return returnList;
	}

	private List<DBQueryRow> performNativeRecursiveQuery(DBDatabase database, RecursiveQueryDetails<T> recursiveDetails, RecursiveSQLDirection direction, List<DBQueryRow> returnList) throws SQLException, UnableToInstantiateDBRowSubclassException {
//		final DBDatabase database = originalQuery.getReadyDatabase();
		final DBDefinition defn = database.getDefinition();
		DBStatement dbStatement = database.getDBStatement();
		final DBQuery originalQuery = recursiveDetails.getOriginalQuery();
		try {
			String descendingQuery = getRecursiveSQL(database, recursiveDetails, recursiveDetails.getKeyToFollow(), direction);
			originalQuery.setTimeoutInMilliseconds(recursiveDetails.getTimeoutInMilliseconds());
			final QueryDetails queryDetails = originalQuery.getQueryDetails();
			ResultSet resultSet = queryDetails.getResultSetForSQL(dbStatement, descendingQuery);
			try {
				while (resultSet.next()) {
					DBQueryRow queryRow = new DBQueryRow(queryDetails);

					originalQuery.setExpressionColumns(defn, resultSet, queryRow);

					queryDetails.setQueryRowFromResultSet(defn,
							resultSet, queryDetails,
							queryRow,
							queryDetails.getDBReportGroupByColumns().size() > 0
					);
					returnList.add(queryRow);
				}
			} finally {
				resultSet.close();
			}
		} finally {
			dbStatement.close();
		}
		return returnList;
	}

	private String getRecursiveSQL(DBDatabase database, RecursiveQueryDetails<T> details, ColumnProvider foreignKeyToFollow, RecursiveSQLDirection direction) {
		final Class<? extends DBRow> referencedClass = foreignKeyToFollow.getColumn().getPropertyWrapper().referencedClass();
		try {
			DBDefinition defn = database.getDefinition();
			final DBRow newInstance = referencedClass.newInstance();
			final String recursiveTableAlias = database.getDefinition().getTableAlias(newInstance);
			String recursiveColumnNames = "";
			StringBuilder recursiveAliases = new StringBuilder();
			final RowDefinitionInstanceWrapper rowDefinitionInstanceWrapper = foreignKeyToFollow.getColumn().getPropertyWrapper().getRowDefinitionInstanceWrapper();
			RowDefinition adapteeRowDefinition = rowDefinitionInstanceWrapper.adapteeRowDefinition();
			List<PropertyWrapper> propertyWrappers = adapteeRowDefinition.getColumnPropertyWrappers();
			String separator = "";
			for (PropertyWrapper propertyWrapper : propertyWrappers) {
				for (PropertyWrapperDefinition.ColumnAspects entry : propertyWrapper.getColumnAspects(database.getDefinition())) {
					String alias = entry.columnAlias;
					final String columnName = defn.formatColumnName(propertyWrapper.columnName());
					recursiveColumnNames += separator + columnName;
					recursiveAliases.append(separator).append(columnName).append(" ").append(alias);
					separator = ", ";
				}
			}
			recursiveColumnNames += separator + defn.getRecursiveQueryDepthColumnName();

			final DBQuery primingSubQueryForRecursiveQuery = getPrimingSubQueryForRecursiveQuery(database, details, foreignKeyToFollow);
			final DBQuery recursiveSubQuery = getRecursiveSubQuery(database, details, recursiveTableAlias, foreignKeyToFollow, direction);

			String recursiveQuery
					= defn.beginWithClause() + defn.formatWithClauseTableDefinition(recursiveTableAlias, recursiveColumnNames)
					+ defn.beginWithClausePrimingQuery()
					+ removeTrailingSemicolon(primingSubQueryForRecursiveQuery.getSQLForQuery())
					+ defn.endWithClausePrimingQuery()
					+ defn.beginWithClauseRecursiveQuery()
					+ removeTrailingSemicolon(recursiveSubQuery.getSQLForQuery())
					+ defn.endWithClauseRecursiveQuery()
					+ defn.doSelectFromRecursiveTable(recursiveTableAlias, recursiveAliases.toString());
			return recursiveQuery;
		} catch (InstantiationException ex) {
			throw new UnableToInstantiateDBRowSubclassException(referencedClass, ex);
		} catch (IllegalAccessException ex) {
			throw new UnableToInstantiateDBRowSubclassException(referencedClass, ex);
		}
	}

	private String removeTrailingSemicolon(String sql) {
		return sql.replaceAll("[ \\t\\r\\n]*;[ \\t\\r\\n]*$", System.getProperty("line.separator"));
	}

	private DBQuery getPrimingSubQueryForRecursiveQuery(DBDatabase database, RecursiveQueryDetails<T> recursiveDetails, ColumnProvider foreignKeyToFollow) {
		DBQuery newQuery = database.getDBQuery();
		final RowDefinitionInstanceWrapper rowDefinitionInstanceWrapper = foreignKeyToFollow.getColumn().getPropertyWrapper().getRowDefinitionInstanceWrapper();
		final Class<?> originatingClass = rowDefinitionInstanceWrapper.adapteeRowDefinitionClass();
		final QueryDetails details = recursiveDetails.getOriginalQuery().getQueryDetails();

		List<DBRow> tables = details.getRequiredQueryTables();
		for (DBRow table : tables) {
			DBRow copied = DBRow.copyDBRow(table);
			if (!originatingClass.equals(table.getClass())) {
				copied.setReturnFieldsToNone();
			}
			newQuery.add(copied);
		}
		tables = details.getOptionalQueryTables();
		for (DBRow table : tables) {
			DBRow copied = DBRow.copyDBRow(table);
			if (!originatingClass.equals(table.getClass())) {
				copied.setReturnFieldsToNone();
			}
			newQuery.addOptional(copied);
		}
		tables = details.getAssumedQueryTables();
		for (DBRow table : tables) {
			DBRow copied = DBRow.copyDBRow(table);
			if (!originatingClass.equals(table.getClass())) {
				copied.setReturnFieldsToNone();
			}
			newQuery.addAssumedTables(copied);
		}
		newQuery.addExpressionColumn(
				database.getDefinition().getRecursiveQueryDepthColumnName(),
				IntegerExpression.value(1).asExpressionColumn()
		);

		return newQuery;
	}

	private DBQuery getRecursiveSubQuery(DBDatabase database, RecursiveQueryDetails<T> recursiveDetails, String recursiveTableAlias, ColumnProvider foreignKeyToFollow, RecursiveSQLDirection direction) {
		Class<? extends DBRow> referencedClass;
		DBQuery newQuery = database.getDBQuery();

		final AbstractColumn fkColumn = foreignKeyToFollow.getColumn();
		referencedClass = fkColumn.getClassReferencedByForeignKey();
		try {
			final DBRow referencedRow = referencedClass.newInstance();

			DBRow originatingRow = fkColumn.getInstanceOfRow();

			referencedRow.setReturnFieldsToNone();
			if (database.getDefinition().requiresRecursiveTableAlias()) {
				referencedRow.setRecursiveTableAlias(recursiveTableAlias);
			}
			if (direction == RecursiveSQLDirection.TOWARDS_ROOT) {
				originatingRow.ignoreAllForeignKeys();
				referencedRow.ignoreAllForeignKeys();
			}
			newQuery.add(originatingRow);
			newQuery.add(referencedRow);

			if (direction == RecursiveSQLDirection.TOWARDS_ROOT) {
				addAscendingExpressionToQuery(recursiveDetails, originatingRow, foreignKeyToFollow, referencedRow, newQuery);
			}

			newQuery.addExpressionColumn(database.getDefinition().getRecursiveQueryDepthColumnName(),
					new RecursiveQueryDepthIncreaseExpression().asExpressionColumn()
			);

		} catch (InstantiationException ex) {
			throw new UnableToInstantiateDBRowSubclassException(referencedClass, ex);
		} catch (IllegalAccessException ex) {
			throw new UnableToInstantiateDBRowSubclassException(referencedClass, ex);
		}

		return newQuery;
	}

	@SuppressWarnings("unchecked")
	private void addAscendingExpressionToQuery(RecursiveQueryDetails<T> recursiveDetails, DBRow originatingRow, ColumnProvider foreignKeyToFollow, final DBRow referencedRow, DBQuery newQuery) throws IncorrectRowProviderInstanceSuppliedException {
		final List<QueryableDatatype<?>> primaryKeys = originatingRow.getPrimaryKeys();
		for (QueryableDatatype<?> primaryKey : primaryKeys) {
			final ColumnProvider pkColumn = originatingRow.column(primaryKey);
			final QueryableDatatype<?> qdt = foreignKeyToFollow.getColumn().getAppropriateQDTFromRow(referencedRow);
			if ((qdt instanceof DBNumber) && (pkColumn instanceof EqualComparable) && (primaryKey instanceof NumberResult)) {
				DBNumber fkValue = (DBNumber) qdt;
				NumberColumn newFKColumn = referencedRow.column(fkValue);
				newQuery.addCondition(
						((EqualComparable<NumberResult>) pkColumn)
								.is(newFKColumn));
			} else if ((qdt instanceof DBInteger) && (pkColumn instanceof EqualComparable) && (primaryKey instanceof IntegerResult)) {
				DBInteger fkValue = (DBInteger) qdt;
				IntegerColumn newFKColumn = referencedRow.column(fkValue);
				newQuery.addCondition(
						((EqualComparable<IntegerResult>) pkColumn)
								.is(newFKColumn));
			} else if ((qdt instanceof DBString) && (pkColumn instanceof EqualComparable) && (primaryKey instanceof StringResult)) {
				DBString fkValue = (DBString) qdt;
				StringColumn newFKColumn = referencedRow.column(fkValue);
				newQuery.addCondition(
						((EqualComparable<StringResult>) pkColumn)
								.is(newFKColumn));
			} else if ((qdt instanceof DBDate) && (pkColumn instanceof EqualComparable) && (primaryKey instanceof DateResult)) {
				DBDate fkValue = (DBDate) qdt;
				DateColumn newFKColumn = referencedRow.column(fkValue);
				newQuery.addCondition(
						((EqualComparable<DateResult>) pkColumn)
								.is(newFKColumn));
			} else {
				throw new nz.co.gregs.dbvolution.exceptions.UnableToCreateAscendingExpressionForRecursiveQuery(recursiveDetails.getKeyToFollow(), originatingRow);
			}
		}
	}

	private List<DBQueryRow> performRecursiveQueryEmulation(DBDatabase database, RecursiveQueryDetails<T> recursiveDetails, RecursiveSQLDirection direction) throws SQLException {

		final T returnType = getReturnType(recursiveDetails);
		List<DBQueryRow> returnList = new ArrayList<>();
		Integer timeout = recursiveDetails.getTimeoutInMilliseconds();
		long start = new java.util.Date().getTime();
		final DBQuery originalQuery = recursiveDetails.getOriginalQuery();
		originalQuery.setTimeoutInMilliseconds(timeout);
		List<DBQueryRow> primingRows = originalQuery.getAllRows();

		Map<String, List<String>> pkValues = new HashMap<>();
		Map<String, PropertyWrapperDefinition> pkDefs = new HashMap<>();

		for (DBQueryRow row : primingRows) {
			final T tab = row.get(returnType);
			List<QueryableDatatype<?>> qdts = tab.getPrimaryKeys();
			for (QueryableDatatype<?> qdt : qdts) {
				final PropertyWrapperDefinition propDefn = tab.getPropertyWrapperOf(qdt).getPropertyWrapperDefinition();
				if (!pkValues.containsKey(propDefn.toString())) {
					pkValues.put(propDefn.toString(), new ArrayList<String>());
					pkDefs.put(propDefn.toString(), propDefn);
				}
				if (!qdt.isNull()) {
					String stringValue = qdt.stringValue();
					pkValues.get(propDefn.toString()).add(stringValue);
				}
			}
		}
		final ColumnProvider keyToFollow = recursiveDetails.getKeyToFollow();
		DBRow instanceOfRow = keyToFollow.getColumn().getInstanceOfRow();
		for (Map.Entry<String, List<String>> entry : pkValues.entrySet()) {
			String key = entry.getKey();
			PropertyWrapperDefinition def = pkDefs.get(key);
			List<String> value = entry.getValue();
			setQDTPermittedValues(def.getQueryableDatatype(instanceOfRow), value);
		}

		final DBQuery dbQuery = database.getDBQuery(instanceOfRow);
		dbQuery.setTimeoutInMilliseconds((int) (timeout - (new java.util.Date().getTime() - start)));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		while (allRows.size() > 0) {
			List<String> recurseValues = new ArrayList<>();
			returnList.addAll(allRows);
			for (DBQueryRow row : allRows) {
				final T tab = row.get(getReturnType(recursiveDetails));
				QueryableDatatype<?> qdt;
				if (direction.equals(RecursiveSQLDirection.TOWARDS_ROOT)) {
					qdt = keyToFollow.getColumn().getAppropriateQDTFromRow(tab);
					if (!qdt.isNull()) {
						recurseValues.add(qdt.stringValue());
					}
				} else {
					List<QueryableDatatype<?>> primaryKeys = tab.getPrimaryKeys();
					for (QueryableDatatype<?> pk : primaryKeys) {
						if (!pk.isNull()) {
							recurseValues.add(pk.stringValue());
						}
					}
				}
			}

			if (recurseValues.isEmpty()) {
				allRows.clear();
			} else {
				instanceOfRow = keyToFollow.getColumn().getInstanceOfRow();
				if (instanceOfRow.getPrimaryKeys().size() > 1) {
					throw new UnableToInterpolateReferencedColumnInMultiColumnPrimaryKeyException(instanceOfRow, instanceOfRow.getPrimaryKeys());
				}
				QueryableDatatype<?> qdt;
				if (direction.equals(RecursiveSQLDirection.TOWARDS_ROOT)) {
					qdt = instanceOfRow.getPrimaryKeys().get(0);
				} else {
					qdt = keyToFollow.getColumn().getAppropriateQDTFromRow(instanceOfRow);
				}
				setQDTPermittedValues(qdt, recurseValues);
				final DBQuery dbQuery1 = database.getDBQuery(instanceOfRow);
				dbQuery1.setTimeoutInMilliseconds((int) (timeout - (new java.util.Date().getTime() - start)));
				allRows = dbQuery1.getAllRows();
			}
		}

		return returnList;
	}

	@SuppressWarnings("unchecked")
	private T getReturnType(RecursiveQueryDetails<T> details) {
		T returnInstance = details.getTypeToReturn();
		ColumnProvider follow = details.getKeyToFollow();
		if (returnInstance == null) {
			final DBRow instanceOfRow = follow.getColumn().getInstanceOfRow();
			Class<? extends DBRow> classReferenceByForeignKey = follow.getColumn().getClassReferencedByForeignKey();
			if (classReferenceByForeignKey == null) {
				throw new ColumnProvidedMustBeAForeignKey(follow);
			}

			returnInstance = (T) instanceOfRow;
		}
		return returnInstance;
	}

	private void setQDTPermittedValues(QueryableDatatype<?> primaryKey, List<String> values) {
		if (primaryKey instanceof DBInteger) {
			DBInteger qdt = (DBInteger) primaryKey;
			List<Long> longs = new ArrayList<>();
			for (String value : values) {
				longs.add(Long.parseLong(value));
			}
			qdt.permittedValues(longs);
		} else if (primaryKey instanceof DBNumber) {
			DBNumber qdt = (DBNumber) primaryKey;
			List<Number> longs = new ArrayList<>();
			for (String value : values) {
				longs.add(Double.parseDouble(value));
			}
			qdt.permittedValues(longs);
		} else if (primaryKey instanceof DBInteger) {
			DBInteger qdt = (DBInteger) primaryKey;
			List<Long> longs = new ArrayList<>();
			for (String value : values) {
				longs.add(Long.parseLong(value));
			}
			qdt.permittedValues(longs);
		} else if (primaryKey instanceof DBString) {
			DBString qdt = (DBString) primaryKey;
			qdt.permittedValues(values);
		} else {
			throw new UnsupportedOperationException("Only Integer, Number, and String Primary Keys are supported.");
		}
	}

}
