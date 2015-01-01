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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.exceptions.*;
import java.sql.*;
import java.util.*;
import nz.co.gregs.dbvolution.columns.*;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import nz.co.gregs.dbvolution.expressions.*;
import nz.co.gregs.dbvolution.internal.properties.*;
import nz.co.gregs.dbvolution.internal.query.RecursiveQueryDepthIncreaseExpression;
import nz.co.gregs.dbvolution.query.*;

public class DBRecursiveQuery<T extends DBRow> {

	private final DBQuery originalQuery;
	private final ColumnProvider keyToFollow;
	private T typeToReturn = null;

	private static enum RecursiveSQLDirection {

		TOWARDS_ROOT, TOWARDS_LEAVES
	};

	public DBRecursiveQuery(DBQuery query, ColumnProvider keyToFollow)throws ColumnProvidedMustBeAForeignKey{
		this.originalQuery = query;
		this.keyToFollow = keyToFollow;
		final Class<? extends DBRow> classReferencedByForeignKey = keyToFollow.getColumn().getClassReferencedByForeignKey();
		if (classReferencedByForeignKey==null){
			throw new ColumnProvidedMustBeAForeignKey(keyToFollow);
		}
		List<DBRow> allTables = query.getAllTables();
		boolean found = false;
		for (DBRow tab : allTables) {
			found = found || (tab.getClass().isAssignableFrom(classReferencedByForeignKey));
		}
		if (! found ){
			throw new ForeignKeyDoesNotReferenceATableInTheQuery(keyToFollow);
		}
		DBRow instanceOfRow = keyToFollow.getColumn().getInstanceOfRow();
		if (!
				(classReferencedByForeignKey.isAssignableFrom(instanceOfRow.getClass())
				||
				instanceOfRow.getClass().isAssignableFrom(classReferencedByForeignKey))
				)
		{throw new ForeignKeyIsNotRecursiveException(keyToFollow);
		}
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
	 * @param foreignKeyToFollow
	 * @param direction
	 * @return A linked List
	 * @throws java.sql.SQLException
	 */
	private List<DBQueryRow> getRowsFromRecursiveQuery(ColumnProvider foreignKeyToFollow, RecursiveSQLDirection direction) throws SQLException {
		List<DBQueryRow> returnList = new ArrayList<DBQueryRow>();
		if (originalQuery.getDatabase().supportsRecursiveQueriesNatively()) {
			DBStatement dbStatement = originalQuery.getDatabase().getDBStatement();
			try {
				String descendingQuery = getRecursiveSQL(foreignKeyToFollow, direction);
				ResultSet resultSet = originalQuery.getResultSetForSQL(dbStatement, descendingQuery);
				try {
					while (resultSet.next()) {
						DBQueryRow queryRow = new DBQueryRow(originalQuery);

						originalQuery.setExpressionColumns(resultSet, queryRow);

						originalQuery.setQueryRowFromResultSet(resultSet, queryRow, originalQuery.getQueryDetails().getDbReportGroupByColumns().size() > 0);
						returnList.add(queryRow);
					}
				} finally {
					resultSet.close();
				}
			} finally {
				dbStatement.close();
			}
			return returnList;

		} else {

		}
		return returnList;
	}

	private String getRecursiveSQL(ColumnProvider foreignKeyToFollow, RecursiveSQLDirection direction) {
		final Class<? extends DBRow> referencedClass = foreignKeyToFollow.getColumn().getPropertyWrapper().referencedClass();
		try {
			final DBDatabase database = originalQuery.getDatabase();
			DBDefinition defn = database.getDefinition();
			final DBRow newInstance = referencedClass.newInstance();
			final String recursiveTableAlias = database.getDefinition().getTableAlias(newInstance);
			String recursiveColumnNames = "";
			String recursiveAliases = "";
			final RowDefinitionInstanceWrapper rowDefinitionInstanceWrapper = foreignKeyToFollow.getColumn().getPropertyWrapper().getRowDefinitionInstanceWrapper();
			RowDefinition adapteeRowDefinition = rowDefinitionInstanceWrapper.adapteeRowDefinition();
			List<PropertyWrapper> propertyWrappers = adapteeRowDefinition.getPropertyWrappers();
			String separator = "";
			for (PropertyWrapper propertyWrapper : propertyWrappers) {
				final String columnName = defn.formatColumnName(propertyWrapper.getDefinition().getColumnName());
				recursiveColumnNames += separator + columnName;
				recursiveAliases += separator + columnName + " " + propertyWrapper.getColumnAlias(database);
				separator = ", ";
			}
			recursiveColumnNames += separator + defn.getRecursiveQueryDepthColumnName();

			final DBQuery primingSubQueryForRecursiveQuery = this.getPrimingSubQueryForRecursiveQuery(foreignKeyToFollow);
			final DBQuery recursiveSubQuery = this.getRecursiveSubQuery(recursiveTableAlias, foreignKeyToFollow, direction);

			String recursiveQuery
					= defn.beginWithClause() + defn.formatWithClauseTableDefinition(recursiveTableAlias, recursiveColumnNames)
					+ defn.beginWithClausePrimingQuery()
					+ removeTrailingSemicolon(primingSubQueryForRecursiveQuery.getSQLForQuery())
					+ defn.endWithClausePrimingQuery()
					+ defn.beginWithClauseRecursiveQuery()
					+ removeTrailingSemicolon(recursiveSubQuery.getSQLForQuery())
					+ defn.endWithClauseRecursiveQuery()
					+ defn.doSelectFromRecursiveTable(recursiveTableAlias, recursiveAliases);
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

	private DBQuery getPrimingSubQueryForRecursiveQuery(ColumnProvider foreignKeyToFollow) {
		final DBDatabase database = originalQuery.getDatabase();
		DBQuery newQuery = database.getDBQuery();
		final RowDefinitionInstanceWrapper rowDefinitionInstanceWrapper = foreignKeyToFollow.getColumn().getPropertyWrapper().getRowDefinitionInstanceWrapper();
		final Class<?> originatingClass = rowDefinitionInstanceWrapper.adapteeRowDefinitionClass();
		final QueryDetails details = originalQuery.getQueryDetails();

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
				NumberExpression.value(1)
		);

		return newQuery;
	}

	private DBQuery getRecursiveSubQuery(String recursiveTableAlias, ColumnProvider foreignKeyToFollow, RecursiveSQLDirection direction) {
		Class<? extends DBRow> referencedClass = DBRow.class;
		final DBDatabase database = originalQuery.getDatabase();
		DBQuery newQuery = database.getDBQuery();
		try {

			final AbstractColumn fkColumn = foreignKeyToFollow.getColumn();
			referencedClass = fkColumn.getClassReferencedByForeignKey();
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
				addAscendingExpressionToQuery(originatingRow, foreignKeyToFollow, referencedRow, newQuery);
			}

			newQuery.addExpressionColumn(database.getDefinition().getRecursiveQueryDepthColumnName(),
					new RecursiveQueryDepthIncreaseExpression()
			);

		} catch (InstantiationException ex) {
			throw new UnableToInstantiateDBRowSubclassException(referencedClass, ex);
		} catch (IllegalAccessException ex) {
			throw new UnableToInstantiateDBRowSubclassException(referencedClass, ex);
		}

		return newQuery;
	}

	@SuppressWarnings("unchecked")
	private void addAscendingExpressionToQuery(DBRow originatingRow, ColumnProvider foreignKeyToFollow, final DBRow referencedRow, DBQuery newQuery) throws IncorrectRowProviderInstanceSuppliedException {
		final QueryableDatatype primaryKey = originatingRow.getPrimaryKey();
		final ColumnProvider pkColumn = originatingRow.column(primaryKey);
		final QueryableDatatype qdt = foreignKeyToFollow.getColumn().getAppropriateQDTFromRow(referencedRow);
		if ((qdt instanceof DBNumber) && (pkColumn instanceof EqualComparable) && (primaryKey instanceof NumberResult)) {
			DBNumber fkValue = (DBNumber) qdt;
			NumberColumn newFKColumn = referencedRow.column(fkValue);
			newQuery.addCondition(
					((EqualComparable<NumberResult>) pkColumn)
					.is(newFKColumn));
		} else if ((qdt instanceof DBInteger) && (pkColumn instanceof EqualComparable) && (primaryKey instanceof NumberResult)) {
			DBInteger fkValue = (DBInteger) qdt;
			IntegerColumn newFKColumn = referencedRow.column(fkValue);
			newQuery.addCondition(
					((EqualComparable<NumberResult>) pkColumn)
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
		}else{
			throw new nz.co.gregs.dbvolution.exceptions.UnableToCreateAscendingExpressionForRecursiveQuery(keyToFollow, originatingRow);
		}
	}

	public List<T> getDescendants() throws SQLException {
		List<T> resultsList = new ArrayList<T>();
		List<DBQueryRow> descendants = this.getRowsFromRecursiveQuery(this.keyToFollow, RecursiveSQLDirection.TOWARDS_LEAVES);
		for (DBQueryRow descendant : descendants) {
			resultsList.add(descendant.get(getReturnType()));
		}
		return resultsList;
	}

	public List<T> getAncestors() throws SQLException {
		List<T> resultsList = new ArrayList<T>();
		List<DBQueryRow> ancestors = this.getRowsFromRecursiveQuery(this.keyToFollow, RecursiveSQLDirection.TOWARDS_ROOT);
		for (DBQueryRow ancestor : ancestors) {
			resultsList.add(ancestor.get(getReturnType()));
		}
		return resultsList;
	}

	@SuppressWarnings("unchecked")
	private T getReturnType() {
		if (typeToReturn == null) {
			final DBRow instanceOfRow = this.keyToFollow.getColumn().getInstanceOfRow();
			Class<? extends DBRow> classReferenceByForeignKey = this.keyToFollow.getColumn().getClassReferencedByForeignKey();
			if (classReferenceByForeignKey == null) {
				throw new ColumnProvidedMustBeAForeignKey(keyToFollow);
			}

			this.typeToReturn = (T) instanceOfRow;
		}
		return this.typeToReturn;
	}

	/**
	 * Creates a List from the rows returned by this query to a root node of the
	 * example table by repeatedly following the recursive foreign key provided.
	 *
	 * <p>
	 * Tree structures are stored in databases using a table with a foreign key to
	 * the same table (the aforementioned "recursive foreign key"). This method
	 * provides a simple means of the traversing the stored tree structure to find
	 * the path to the root node.
	 *
	 * <p>
	 * Where possible DBvolution uses recursive queries to traverse the tree.
	 *
	 * <p>
	 * Recursive queries are only possible on tables that are part of this query,
	 * and have a foreign key that directly references rows within the table
	 * itself.
	 *
	 * <p>
	 * In DBvolution it is common to reference a subclass of the table to add
	 * semantic information and help complex query creation. As such sub-classed
	 * foreign keys are fully supported.
	 *
	 * @return a list of the ancestors of the results from this query.
	 * @throws SQLException
	 */
	public List<T> getPathToRoot() throws SQLException {
		List<T> ancestors = getAncestors();
		return ancestors;
	}


}
