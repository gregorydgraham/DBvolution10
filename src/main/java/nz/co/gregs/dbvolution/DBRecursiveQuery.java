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
import nz.co.gregs.dbvolution.exceptions.*;
import java.sql.*;
import java.util.*;
import nz.co.gregs.dbvolution.columns.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.internal.query.RecursiveQueryDetails;
import nz.co.gregs.dbvolution.query.*;

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

	private final RecursiveQueryDetails<T> queryDetails = new RecursiveQueryDetails<>();

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
	public synchronized DBRecursiveQuery<T> setTimeoutInMilliseconds(Integer timeoutInMilliseconds) {
		this.queryDetails.setTimeoutInMilliseconds(timeoutInMilliseconds);
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
	public synchronized DBRecursiveQuery<T> clearTimeout() {
		this.queryDetails.setTimeoutToDefault();
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
		this.queryDetails.setOriginalQuery(query);
		this.queryDetails.setKeyToFollow(keyToFollow);
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
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public synchronized List<T> getDescendants() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<T> resultsList = new ArrayList<>();
		queryDetails.setRecursiveQueryDirection(RecursiveSQLDirection.TOWARDS_LEAVES);
		List<DBQueryRow> descendants = this.getRowsFromRecursiveQuery(queryDetails);
		for (DBQueryRow descendant : descendants) {
			resultsList.add(descendant.get(getReturnType(queryDetails)));
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
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public synchronized List<T> getAncestors() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<T> resultsList = new ArrayList<>();
		this.queryDetails.setRecursiveQueryDirection(RecursiveSQLDirection.TOWARDS_ROOT);
		List<DBQueryRow> ancestors = this.getRowsFromRecursiveQuery(queryDetails);
		for (DBQueryRow ancestor : ancestors) {
			final T got = ancestor.get(getReturnType(queryDetails));
			resultsList.add(got);
		}
		return resultsList;
	}

	@SuppressWarnings("unchecked")
	private synchronized T getReturnType(RecursiveQueryDetails<T> details) {
		T typeToReturn = details.getTypeToReturn();
		ColumnProvider keyToFollow = details.getKeyToFollow();
		if (typeToReturn == null) {
			final DBRow instanceOfRow = keyToFollow.getColumn().getInstanceOfRow();
			Class<? extends DBRow> classReferenceByForeignKey = keyToFollow.getColumn().getClassReferencedByForeignKey();
			if (classReferenceByForeignKey == null) {
				throw new ColumnProvidedMustBeAForeignKey(keyToFollow);
			}

			typeToReturn = (T) instanceOfRow;
		}
		return typeToReturn;
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
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public synchronized List<TreeNode<T>> getPathsToRoot() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<T> ancestors = getAncestors();
		List<TreeNode<T>> paths = new ArrayList<>();
		Map<String, TreeNode<T>> parentMap = new HashMap<>();
		Map<String, List<TreeNode<T>>> childrenMap = new HashMap<>();
		for (T currentRow : ancestors) {
			TreeNode<T> currentNode = new TreeNode<>(currentRow);
			final String parentPKValue = queryDetails.getKeyToFollow().getColumn().getAppropriateQDTFromRow(currentRow).stringValue();
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
	 * @throws nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException
	 */
	public synchronized List<TreeNode<T>> getTrees() throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		List<T> descendants = getDescendants();
		List<TreeNode<T>> trees = new ArrayList<>();
		Map<String, TreeNode<T>> parentMap = new HashMap<>();
		Map<String, List<TreeNode<T>>> childrenMap = new HashMap<>();
		for (T currentRow : descendants) {
			String parentPKValue = queryDetails.getKeyToFollow().getColumn().getAppropriateQDTFromRow(currentRow).stringValue();
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

	private synchronized List<DBQueryRow> getRowsFromRecursiveQuery(RecursiveQueryDetails<T> queryDetails) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException {
		if (queryDetails.needsResults(queryDetails.getOptions())) {
			queryDetails.getOriginalQuery().getDatabase().executeDBQuery(queryDetails);
		}
		return queryDetails.getResults();
	}
}
