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

import nz.co.gregs.dbvolution.internal.query.RecursiveSQLDirection;
import nz.co.gregs.dbvolution.internal.query.QueryDetails;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.results.NumberResult;
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
import nz.co.gregs.dbvolution.results.IntegerResult;

/**
 * Provides the infrastructure required to create recursive queries. DBvolution
 * uses native recursive queries where possible and emulation when necessary.
 *
 * <p>
 * Recursive queries repeatedly access the same table with new data to produce
 * more rows.
 *
 * <p>
 * The two main uses of recursive queries are to retrieve tree data structures
 * stored with in the database or to retrieve a path from the tree structure.
 *
 * <p>
 * Tree structures are stored in relational databases as foreign keys
 * referencing the same table. That is to say: table T includes a foreign key to
 * table T. By following the foreign key we find another row with another
 * foreign key to table T and the process repeats.
 *
 * <p>
 * Recursive queries and structures can also be used to access and store
 * Digraphs but digraphs are not yet supported.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 * @param <T> the table/DBRow that will be returned from the query and is
 * referenced by the foreign key.
 */
public class DBRecursiveQuery<T extends DBRow> {

	private final DBQuery originalQuery;
	private final ColumnProvider keyToFollow;
	private T typeToReturn = null;
	private Integer timeoutInMilliseconds = 10000;

	/**
	 * Changes the default timeout for this query.
	 *
	 * <p>
	 * DBvolution defaults to a timeout of 10000milliseconds (10 seconds) to avoid
	 * eternal queries.
	 *
	 * <p>
	 * Use this method If you require a longer running query.
	 *
	 * @param timeoutInMilliseconds
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return this query.
	 */
	public DBRecursiveQuery<T> setTimeoutInMilliseconds(Integer timeoutInMilliseconds) {
		this.timeoutInMilliseconds = timeoutInMilliseconds;
		return this;
	}

	/**
	 * Changes the default timeout for this query.
	 *
	 * <p>
	 * DBvolution defaults to a timeout of 10000milliseconds (10 seconds) to avoid
	 * eternal queries.
	 *
	 * <p>
	 * Use this method If you expect an extremely long query.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this query.
	 */
	public DBRecursiveQuery<T> clearTimeout() {
		this.timeoutInMilliseconds = null;
		return this;
	}


	/**
	 * Create a DBRecursiveQuery based on the query and foreign key supplied.
	 *
	 * <p>
	 * DBRecursiveQuery uses the query to create the first rows of the recursive
	 * query. This can be any query and contain any tables. However it must
	 * contain the table T and the foreign key must be a recursive FK to and from
	 * table T.
	 *
	 * <p>
	 * After the priming query has been created the foreign key(FK) supplied will
	 * be followed repeatedly. The FK must be contained in one of the tables of
	 * the priming query and it must reference the same table, that is to say it
	 * must be a recursive foreign key.
	 *
	 * <p>
	 * The FK will be repeatedly followed until the root node is reached (an
	 * ascending query) or the leaf nodes have been reached (a descending query).
	 * A root node is defined as a row with a null value in the FK. A leaf node is
	 * a row that has no FKs referencing it.
	 *
	 * <p>
	 * While it is possible to define a root node in other ways only the above
	 * definition is currently supported.
	 *
	 * @param query
	 * @param keyToFollow
	 * @throws ColumnProvidedMustBeAForeignKey
	 * @throws ForeignKeyDoesNotReferenceATableInTheQuery
	 * @throws ForeignKeyIsNotRecursiveException
	 */
	public DBRecursiveQuery(DBQuery query, ColumnProvider keyToFollow) throws ColumnProvidedMustBeAForeignKey, ForeignKeyDoesNotReferenceATableInTheQuery, ForeignKeyIsNotRecursiveException {
		this.originalQuery = query;
		this.keyToFollow = keyToFollow;
		final Class<? extends DBRow> classReferencedByForeignKey = keyToFollow.getColumn().getClassReferencedByForeignKey();
		if (classReferencedByForeignKey == null) {
			throw new ColumnProvidedMustBeAForeignKey(keyToFollow);
		}
		List<DBRow> allTables = query.getAllTables();
		boolean found = false;
		for (DBRow tab : allTables) {
			found = found || (tab.getClass().isAssignableFrom(classReferencedByForeignKey));
		}
		if (!found) {
			throw new ForeignKeyDoesNotReferenceATableInTheQuery(keyToFollow);
		}
		DBRow instanceOfRow = keyToFollow.getColumn().getInstanceOfRow();
		if (!(classReferencedByForeignKey.isAssignableFrom(instanceOfRow.getClass())
				|| instanceOfRow.getClass().isAssignableFrom(classReferencedByForeignKey))) {
			throw new ForeignKeyIsNotRecursiveException(keyToFollow);
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
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A linked List
	 *
	 */
	private List<DBQueryRow> getRowsFromRecursiveQuery(RecursiveSQLDirection direction) throws SQLException {
		List<DBQueryRow> returnList = new ArrayList<>();
		if (originalQuery.getDatabaseDefinition().supportsRecursiveQueriesNatively()) {
			returnList = performNativeRecursiveQuery(direction, returnList);
		} else {
			returnList = performRecursiveQueryEmulation(direction);
		}
		return returnList;
	}

	private List<DBQueryRow> performNativeRecursiveQuery(RecursiveSQLDirection direction, List<DBQueryRow> returnList) throws SQLException, UnableToInstantiateDBRowSubclassException {
		final DBDatabase database = originalQuery.getReadyDatabase();
		final DBDefinition defn = database.getDefinition();
		DBStatement dbStatement = database.getDBStatement();
		try {
			String descendingQuery = getRecursiveSQL(this.keyToFollow, direction);
			originalQuery.setTimeoutInMilliseconds(this.timeoutInMilliseconds);
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

	private String getRecursiveSQL(ColumnProvider foreignKeyToFollow, RecursiveSQLDirection direction) {
		final Class<? extends DBRow> referencedClass = foreignKeyToFollow.getColumn().getPropertyWrapper().referencedClass();
		try {
			final DBDatabase database = originalQuery.getReadyDatabase();
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

	private DBQuery getPrimingSubQueryForRecursiveQuery(ColumnProvider foreignKeyToFollow) {
		final DBDatabase database = originalQuery.getReadyDatabase();
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
				IntegerExpression.value(1).asExpressionColumn()
		);

		return newQuery;
	}

	private DBQuery getRecursiveSubQuery(String recursiveTableAlias, ColumnProvider foreignKeyToFollow, RecursiveSQLDirection direction) {
		Class<? extends DBRow> referencedClass;
		final DBDatabase database = originalQuery.getReadyDatabase();
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
				addAscendingExpressionToQuery(originatingRow, foreignKeyToFollow, referencedRow, newQuery);
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
	private void addAscendingExpressionToQuery(DBRow originatingRow, ColumnProvider foreignKeyToFollow, final DBRow referencedRow, DBQuery newQuery) throws IncorrectRowProviderInstanceSuppliedException {
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
				throw new nz.co.gregs.dbvolution.exceptions.UnableToCreateAscendingExpressionForRecursiveQuery(keyToFollow, originatingRow);
			}
		}
	}

	/**
	 * Queries that database and returns all descendants including priming and
	 * leaf nodes of this query.
	 *
	 * <p>
	 * Using this DBRecursiveQuery as a basis, this method descends the tree
	 * structure finding all descendents of the rows returned by the priming
	 * query. This is used by {@link #getTrees() } to recreate the tree structure
	 * stored in the database as a tree of {@link TreeNode TreeNodes}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of all descendants of this query.
	 * @throws SQLException
	 */
	public List<T> getDescendants() throws SQLException {
		List<T> resultsList = new ArrayList<>();
		List<DBQueryRow> descendants = this.getRowsFromRecursiveQuery(RecursiveSQLDirection.TOWARDS_LEAVES);
		for (DBQueryRow descendant : descendants) {
			resultsList.add(descendant.get(getReturnType()));
		}
		return resultsList;
	}

	/**
	 * Queries that database and returns all ancestors including priming and root
	 * nodes of this query.
	 *
	 * <p>
	 * Using this DBRecursiveQuery as a basis, this method ascends the tree
	 * structure finding all ancestors of the rows returned by the priming query.
	 * This is used by {@link #getPathsToRoot() } to recreate the paths stored in
	 * the database as a list of {@link TreeNode TreeNodes}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of all descendants of this query.
	 * @throws SQLException
	 */
	public List<T> getAncestors() throws SQLException {
		List<T> resultsList = new ArrayList<>();
		List<DBQueryRow> ancestors = this.getRowsFromRecursiveQuery(RecursiveSQLDirection.TOWARDS_ROOT);
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
	 * Creates a List of paths (actually non-branching trees) from the rows
	 * returned by this query to a root node of the example table by repeatedly
	 * following the recursive foreign key provided.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of the ancestors of the results from this query. 1 Database
	 * exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public List<TreeNode<T>> getPathsToRoot() throws SQLException {
		List<T> ancestors = getAncestors();
		List<TreeNode<T>> paths = new ArrayList<>();
		Map<String, TreeNode<T>> parentMap = new HashMap<>();
		Map<String, List<TreeNode<T>>> childrenMap = new HashMap<>();
		for (T currentRow : ancestors) {
			TreeNode<T> currentNode = new TreeNode<>(currentRow);
			final String parentPKValue = keyToFollow.getColumn().getAppropriateQDTFromRow(currentRow).stringValue();
			TreeNode<T> parent = parentMap.get(parentPKValue);
			if (parent != null) {
				parent.addChild(currentNode);
			} else {
				List<TreeNode<T>> listOfChildren = childrenMap.get(parentPKValue);
				if (listOfChildren == null) {
					listOfChildren = new ArrayList<>();
					childrenMap.put(parentPKValue, listOfChildren);
				}
				listOfChildren.add(currentNode);
			}
			final List<QueryableDatatype<?>> primaryKeys = currentRow.getPrimaryKeys();
			for (QueryableDatatype<?> primaryKey : primaryKeys) {
				String pkValue = primaryKey.stringValue();
				List<TreeNode<T>> children = childrenMap.get(pkValue);
				if (children != null) {
					for (TreeNode<T> child : children) {
						currentNode.addChild(child);
					}
					childrenMap.remove(pkValue);
				}
				parentMap.put(pkValue, currentNode);
				parent = currentNode.getParent();
				if (parent != null) {
					paths.remove(parent);
				}
				if (currentNode.getChildren().isEmpty()) {
					paths.add(currentNode);
				}
			}
		}
		return paths;
	}

	/**
	 * Creates a list of trees from the rows returned by this query to to the leaf
	 * nodes of the example table by repeatedly following the recursive foreign
	 * key provided.
	 *
	 * <p>
	 * Tree structures are stored in databases using a table with a foreign key to
	 * the same table (the aforementioned "recursive foreign key"). This method
	 * provides a simple means of the traversing the stored tree structure to find
	 * the path to the leaves.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of trees of the descendants of the results from this query.
	 * 1 Database exceptions may be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public List<TreeNode<T>> getTrees() throws SQLException {
		List<T> descendants = getDescendants();
		List<TreeNode<T>> trees = new ArrayList<>();
		Map<String, TreeNode<T>> parentMap = new HashMap<>();
		Map<String, List<TreeNode<T>>> childrenMap = new HashMap<>();
		for (T currentRow : descendants) {
			String parentPKValue = keyToFollow.getColumn().getAppropriateQDTFromRow(currentRow).stringValue();
			final List<QueryableDatatype<?>> pks = currentRow.getPrimaryKeys();
			for (QueryableDatatype<?> pk : pks) {
				String pkValue = pk.stringValue();
				TreeNode<T> currentNode = new TreeNode<>(currentRow);
				List<TreeNode<T>> children = childrenMap.get(pkValue);
				if (children != null) {
					for (TreeNode<T> child : children) {
						currentNode.addChild(child);
						trees.remove(child);
					}
				}
				parentMap.put(pkValue, currentNode);
				TreeNode<T> parent = parentMap.get(parentPKValue);
				if (parent != null) {
					parent.addChild(currentNode);
				} else {
					List<TreeNode<T>> listOfChildren = childrenMap.get(parentPKValue);
					if (listOfChildren == null) {
						listOfChildren = new ArrayList<>();
						childrenMap.put(parentPKValue, listOfChildren);
					}
					listOfChildren.add(currentNode);
				}
				if (currentNode.getParent() == null) {
					trees.add(currentNode);
				} else {
					trees.remove(currentNode);
				}
			}
		}
		return trees;
	}

	private List<DBQueryRow> performRecursiveQueryEmulation(RecursiveSQLDirection direction) throws SQLException {

		final T returnType = getReturnType();
		List<DBQueryRow> returnList = new ArrayList<>();
		Integer timeout = this.timeoutInMilliseconds;
		long start = new java.util.Date().getTime();
		this.originalQuery.setTimeoutInMilliseconds(timeout);
		List<DBQueryRow> primingRows = this.originalQuery.getAllRows();

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
		DBRow instanceOfRow = this.keyToFollow.getColumn().getInstanceOfRow();
		for (Map.Entry<String, List<String>> entry : pkValues.entrySet()) {
			String key = entry.getKey();
			PropertyWrapperDefinition def = pkDefs.get(key);
			List<String> value = entry.getValue();
			setQDTPermittedValues(def.getQueryableDatatype(instanceOfRow), value);
		}

		final DBQuery dbQuery = this.originalQuery.getReadyDatabase().getDBQuery(instanceOfRow);
		dbQuery.setTimeoutInMilliseconds((int) (timeout - (new java.util.Date().getTime() - start)));
		List<DBQueryRow> allRows = dbQuery.getAllRows();

		while (allRows.size() > 0) {
			List<String> recurseValues = new ArrayList<>();
			returnList.addAll(allRows);
			for (DBQueryRow row : allRows) {
				final T tab = row.get(getReturnType());
				QueryableDatatype<?> qdt;
				if (direction.equals(RecursiveSQLDirection.TOWARDS_ROOT)) {
					qdt = this.keyToFollow.getColumn().getAppropriateQDTFromRow(tab);
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
				instanceOfRow = this.keyToFollow.getColumn().getInstanceOfRow();
				if (instanceOfRow.getPrimaryKeys().size() > 1) {
					throw new UnableToInterpolateReferencedColumnInMultiColumnPrimaryKeyException(instanceOfRow, instanceOfRow.getPrimaryKeys());
				}
				QueryableDatatype<?> qdt;
				if (direction.equals(RecursiveSQLDirection.TOWARDS_ROOT)) {
					qdt = instanceOfRow.getPrimaryKeys().get(0);
				} else {
					qdt = this.keyToFollow.getColumn().getAppropriateQDTFromRow(instanceOfRow);
				}
				setQDTPermittedValues(qdt, recurseValues);
				final DBQuery dbQuery1 = this.originalQuery.getReadyDatabase().getDBQuery(instanceOfRow);
				dbQuery1.setTimeoutInMilliseconds((int) (timeout - (new java.util.Date().getTime() - start)));
				allRows = dbQuery1.getAllRows();
			}
		}

		return returnList;
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
