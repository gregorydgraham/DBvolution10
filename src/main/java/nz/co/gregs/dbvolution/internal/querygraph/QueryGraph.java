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
package nz.co.gregs.dbvolution.internal.querygraph;

import nz.co.gregs.dbvolution.internal.query.DBRowClass;
import edu.uci.ics.jung.graph.SparseMultigraph;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 * A class to create a network of all the classes used in a query, using foreign
 * keys and other relationships.
 *
 * <p>
 * A undirected, possibly-cyclic, database-agnostic, graph of DBRow nodes
 * connected by bi-directional edges created from known foreign keys and the
 * expressions used in the query.
 *
 * <p>
 * This class is used to identify cartesian joins by establishing which tables
 * are included in the join, and which are not. If any node cannot be reached by
 * traversing the graph, a {@link AccidentalCartesianJoinException} may be
 * thrown during querying.
 *
 * <p>
 * Ignored foreign keys are not included as edges.
 *
 * <p>
 * DBRows returned by {@link DBExpression#getTablesInvolved() } will be used to
 * creates edges from expressions in the query.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class QueryGraph {

	private final Map<DBRowClass, QueryGraphNode> nodes = new LinkedHashMap<>();
	private final Map<DBRowClass, DBRow> rows = new LinkedHashMap<>();
	private edu.uci.ics.jung.graph.Graph<QueryGraphNode, DBExpression> jungGraph = new SparseMultigraph<>();

	/**
	 * Create a graph of the tables and connections for all the DBRows and
	 * BooleanExpressions provided.
	 *
	 * @param allQueryTables
	 * @param expressions
	 */
	public QueryGraph(List<DBRow> allQueryTables, List<BooleanExpression> expressions) {
		addAndConnectToRelevant(allQueryTables, expressions);
	}

	/**
	 * Removes all state and prepares the graph for re-initialization.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this QueryGraph.
	 */
	public QueryGraph clear() {
		nodes.clear();
		rows.clear();
		clearDisplayGraph();
		return this;
	}

	/**
	 * Add the provided DBRows/tables and expressions to this QueryGraph,
	 * generating new nodes and edges as required.
	 *
	 * @param otherTables
	 * @param expressions
	 */
	public final void addAndConnectToRelevant(List<DBRow> otherTables, List<BooleanExpression> expressions) {
		addAndConnectToRelevant(otherTables, expressions, true);
	}

	/**
	 * Add the provided DBRows/tables and expressions to this QueryGraph,
	 * generating new nodes and edges as required.
	 *
	 * <p>
	 * The DBrows/tables will be added as optional (that is "outer join" tables)
	 * and displayed as such.
	 *
	 * @param otherTables
	 * @param expressions
	 */
	public final void addOptionalAndConnectToRelevant(List<DBRow> otherTables, List<BooleanExpression> expressions) {
		addAndConnectToRelevant(otherTables, expressions, false);
	}

	/**
	 * Add the provided DBRows/tables and expressions to this QueryGraph,
	 * generating new nodes and edges as required.
	 *
	 * <p>
	 * The DBrows/tables will be added as optional (that is "outer join" tables)
	 * if requiredTables is FALSE.
	 *
	 * @param otherTables
	 * @param expressions
	 * @param requiredTables
	 */
	public final void addAndConnectToRelevant(List<DBRow> otherTables, List<BooleanExpression> expressions, boolean requiredTables) {

		List<DBRow> tablesAdded = new ArrayList<>();
		List<DBRow> tablesRemaining = new ArrayList<>();
		tablesRemaining.addAll(rows.values());
		tablesRemaining.addAll(otherTables);
		clearDisplayGraph();

		while (tablesRemaining.size() > 0) {
			DBRow table1 = tablesRemaining.get(0);
			DBRowClass table1Class = new DBRowClass(table1);
			QueryGraphNode node1 = getOrCreateNode(table1, table1Class, requiredTables);
			for (DBRow table2 : tablesAdded) {
				if (!table1.getClass().equals(table2.getClass())) {
					if (table1.willBeConnectedTo(table2)) {
						DBRowClass table2Class = new DBRowClass(table2);
						QueryGraphNode node2 = getOrCreateNode(table2, table2Class, requiredTables);
						node1.connectTable(table2Class);
						node2.connectTable(table1Class);
						addEdgesToDisplayGraph(table1, node1, table2, node2);
					}
				}
			}
			tablesAdded.add(table1);
			tablesRemaining.remove(table1);
		}

		for (BooleanExpression expr : expressions) {
			Set<DBRow> tables = expr.getTablesInvolved();
			if (tables.size() > 0) {
				DBRow table1 = tables.iterator().next();
				Set<DBRow> tablesToConnectTo = new HashSet<>(tables);
				tablesToConnectTo.remove(table1);
				final DBRowClass table1Class = new DBRowClass(table1);
				final QueryGraphNode node1 = getOrCreateNode(table1, table1Class, requiredTables);
				addNodeToDisplayGraph(node1);
				for (DBRow table2 : tablesToConnectTo) {
					final DBRowClass table2Class = new DBRowClass(table2);
					final QueryGraphNode node2 = getOrCreateNode(table2, table2Class, requiredTables);
					node1.connectTable(table2Class);
					node2.connectTable(table1Class);
					addNodeToDisplayGraph(node2);
					addEdgeToDisplayGraph(node1, node2, expr);
				}
			}
		}
	}

	private QueryGraphNode getOrCreateNode(DBRow table1, DBRowClass table1Class, boolean requiredTables) {
		QueryGraphNode node1 = nodes.get(table1Class);
		if (node1 == null) {
			node1 = new QueryGraphNode(table1Class, requiredTables);
			addNodeToDisplayGraph(node1);
			nodes.put(table1Class, node1);
			rows.put(table1Class, table1);
		}
		return node1;
	}

	private void addNodeToDisplayGraph(QueryGraphNode node1) {
		if (!jungGraph.containsVertex(node1)) {
			jungGraph.addVertex(node1);
		}
	}

	private void addEdgesToDisplayGraph(DBRow table1, QueryGraphNode node1, DBRow table2, QueryGraphNode node2) {
		for (DBExpression fk : table1.getRelationshipsAsBooleanExpressions(table2)) {
			if (!jungGraph.containsEdge(fk)) {
				jungGraph.addEdge(fk, node1, node2);
			}
		}
	}

	private void addEdgeToDisplayGraph(QueryGraphNode node1, QueryGraphNode node2, DBExpression fk) {

		if (!jungGraph.containsEdge(fk)) {
			jungGraph.addEdge(fk, node1, node2);
		}
	}

	/**
	 * Scans the QueryGraph to detect disconnected DBRows/tables and returns TRUE
	 * if a disconnection exists.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the current graph contains a discontinuity which will cause
	 * a cartesian join to occur.
	 */
	public boolean willCreateCartesianJoin() {
		Set<DBRow> returnTables = new HashSet<>();
		returnTables.addAll(toList());

		for (DBRow row : rows.values()) {
			if (!returnTables.contains(row)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Scans the QueryGraph to detect full outer join.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE contains only optional tables, FALSE otherwise.
	 */
	public boolean willCreateFullOuterJoin() {
		Collection<QueryGraphNode> vertices = jungGraph.getVertices();
		for (QueryGraphNode node : vertices) {
			if (node.isRequiredNode()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Get an arbitrary table from the graph with which to start a traversal.
	 *
	 * <p>
	 * This method prefers to return a required (that is an "inner join") table
	 * over an optional, or "outer join", table. It also prefers tables with
	 * actual conditions to unaltered join or leaf tables.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the class of a DBRow from which to start a traversal.
	 */
	private DBRowClass getStartTable() {
		List<QueryGraphNode> innerNodes = new ArrayList<>();
		List<QueryGraphNode> outerNodes = new ArrayList<>();

		for (QueryGraphNode node : nodes.values()) {
			if (node.isRequiredNode()) {
				innerNodes.add(node);
			} else {
				outerNodes.add(node);
			}
		}

		List<QueryGraphNode> nodesToCheck = innerNodes;
		if (innerNodes.isEmpty()) {
			nodesToCheck = outerNodes;
		}

		for (QueryGraphNode queryGraphNode : nodesToCheck) {
			final DBRowClass tableClass = queryGraphNode.getTable();
			final DBRow table = rows.get(tableClass);
			if (table.hasConditionsSet()) {
				return tableClass;
			}
		}
		return nodesToCheck.get(0).getTable();
	}

	/**
	 * Return tables in the QueryGraph as a list.
	 *
	 * <p>
	 * Starting from a semi-random table (see {@link #getStartTable() }) traverse
	 * the graph and add all nodes found to the list.
	 *
	 * <p>
	 * This method does not check for discontinuities. If there is a cartesian
	 * join/discontinuity present only some of the nodes will be returned. Use {@link #toListIncludingCartesian()
	 * } if you need to span a discontinuity.
	 *
	 * <p>
	 * Some optimization is attempted, by trying to include all the required/inner
	 * tables first before adding the optional/outer tables. This avoids a common
	 * problem of a query that spans the intersection of 2 optional/outer tables,
	 * creating mid-query cartesian join that could have been avoided by including
	 * a related required/inner table first.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of all DBRows in this QueryGraph in a smart an order as
	 * possible.
	 */
	public List<DBRow> toList() {
		return toList(getStartTable(), false);
	}

	/**
	 * Return tables in the QueryGraph as a list.
	 *
	 * <p>
	 * Starting from a semi-random table (see {@link #getStartTable() }) traverse
	 * the graph and add all nodes found to the list.
	 *
	 * <p>
	 * This method does not check for discontinuities. If there is a cartesian
	 * join/discontinuity present only some of the nodes will be returned. Use {@link #toListIncludingCartesian()
	 * } if you need to span a discontinuity.
	 *
	 * <p>
	 * Some optimization is attempted, by trying to include all the required/inner
	 * tables first before adding the optional/outer tables. This avoids a common
	 * problem of a query that spans the intersection of 2 optional/outer tables,
	 * creating mid-query cartesian join that could have been avoided by including
	 * a related required/inner table first.
	 *
	 * @param reversed TRUE if the list should be reversed, FALSE otherwise
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of all DBRows in this QueryGraph in a smart an order as
	 * possible.
	 */
	public List<DBRow> toListReversable(boolean reversed) {
		return toList(getStartTable(), reversed);
	}

	@SuppressWarnings("unchecked")
	private List<DBRow> toList(DBRowClass startFrom, boolean reverse) {
		LinkedHashSet<DBRowClass> sortedInnerTables = new LinkedHashSet<>();
		LinkedHashSet<DBRowClass> sortedAllTables = new LinkedHashSet<>();
		List<DBRowClass> addedInnerTables = new ArrayList<>();
		List<DBRowClass> addedAllTables = new ArrayList<>();
		QueryGraphNode nodeA = nodes.get(startFrom);
		sortedAllTables.add(nodeA.getTable());
		int sortedAllBeforeLoop = 0;
		while (sortedAllTables.size() > sortedAllBeforeLoop) {
			sortedAllBeforeLoop = sortedAllTables.size();
			addedAllTables.clear();
			sortedInnerTables.addAll(sortedAllTables);
			int sortedInnerBeforeLoop = 0;
			while (sortedInnerTables.size() > sortedInnerBeforeLoop) {
				sortedInnerBeforeLoop = sortedInnerTables.size();
				addedInnerTables.clear();
				// Reverse the list to make it a depth first search
				DBRowClass[] dummyArray = new DBRowClass[]{};
				DBRowClass[] sortedArray = sortedInnerTables.toArray(dummyArray);
				List<DBRowClass> reversedList = Arrays.asList(sortedArray);
				Collections.reverse(reversedList);
				for (DBRowClass row : reversedList) {
					nodeA = nodes.get(row);
					for (DBRowClass table : nodeA.getConnectedTables()) {
						QueryGraphNode nodeToAdd = nodes.get(table);
						final DBRowClass nodeTable = nodeToAdd.getTable();
						if (nodeToAdd.isRequiredNode()) {
							addedInnerTables.add(nodeTable);
						}
						addedAllTables.add(nodeTable);
					}
				}
				sortedInnerTables.addAll(addedInnerTables);
			}
			sortedAllTables.addAll(addedAllTables);
		}
		List<DBRow> returnTables = new ArrayList<>();
		for (DBRowClass rowClass : sortedInnerTables) {
			returnTables.add(rows.get(rowClass));
		}
		if (reverse) {
			Collections.reverse(returnTables);
		}
		return returnTables;
	}

	/**
	 * Return all tables in the QueryGraph as a list.
	 *
	 * <p>
	 * Starting from a semi-random table (see {@link #getStartTable() }) traverse
	 * the graph and add all nodes found to the list.
	 *
	 * <p>
	 * This method scans across discontinuities. If there is a cartesian
	 * join/discontinuity present all of the nodes will be returned. Use {@link #toList()
	 * } if you need to avoid spanning a discontinuity.
	 *
	 * <p>
	 * Some optimization is attempted, by trying to include all the required/inner
	 * tables first before adding the optional/outer tables. This avoids a common
	 * problem of a query that spans the intersection of 2 optional/outer tables,
	 * creating mid-query cartesian join that could have been avoided by including
	 * a related required/inner table first.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of all DBRows in this QueryGraph in a smart an order as
	 * possible.
	 */
	public List<DBRow> toListIncludingCartesian() {
		return toListIncludingCartesianReversable(false);
	}

	/**
	 * Return all tables in the QueryGraph as a list.
	 *
	 * <p>
	 * Starting from a semi-random table (see {@link #getStartTable() }) traverse
	 * the graph and add all nodes found to the list.
	 *
	 * <p>
	 * This method scans across discontinuities. If there is a cartesian
	 * join/discontinuity present all of the nodes will be returned. Use {@link #toList()
	 * } if you need to avoid spanning a discontinuity.
	 *
	 * <p>
	 * Some optimization is attempted, by trying to include all the required/inner
	 * tables first before adding the optional/outer tables. This avoids a common
	 * problem of a query that spans the intersection of 2 optional/outer tables,
	 * creating mid-query cartesian join that could have been avoided by including
	 * a related required/inner table first.
	 *
	 * @param reverse TRUE if the list needs to be reversed, FALSE otherwise
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a list of all DBRows in this QueryGraph in a smart an order as
	 * possible.
	 */
	public List<DBRow> toListIncludingCartesianReversable(Boolean reverse) {
		Set<DBRow> returnTables = new HashSet<>();

		returnTables.addAll(toList());
		boolean changed = true;

		while (changed) {
			for (DBRow row : rows.values()) {
				changed = false;
				if (!returnTables.contains(row)) {
					returnTables.addAll(toList(new DBRowClass(row), false));
					changed = true;
				}
			}
		}
		final List<DBRow> returnList = new ArrayList<>();
		returnList.addAll(returnTables);
		if (reverse) {
			Collections.reverse(returnList);
		}
		return returnList;
	}

	/**
	 * Create a Jung graph of this QueryGraph for display purposes.
	 *
	 * <p>
	 * Other graphs are available but we use {@link edu.uci.ics.jung.graph.Graph}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a Jung Graph.
	 */
	public edu.uci.ics.jung.graph.Graph<QueryGraphNode, DBExpression> getJungGraph() {
		return jungGraph;
	}

	private void clearDisplayGraph() {
		jungGraph = new SparseMultigraph<>();
	}

}
