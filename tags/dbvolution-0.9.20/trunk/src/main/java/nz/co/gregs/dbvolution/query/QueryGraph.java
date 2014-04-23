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
package nz.co.gregs.dbvolution.query;

import edu.uci.ics.jung.graph.SparseMultigraph;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;

/**
 *
 * @author Gregory Graham
 */
public class QueryGraph {

	final Map<Class<? extends DBRow>, QueryGraphNode> nodes = new LinkedHashMap<Class<? extends DBRow>, QueryGraphNode>();
	final Map<Class<? extends DBRow>, DBRow> rows = new LinkedHashMap<Class<? extends DBRow>, DBRow>();
	edu.uci.ics.jung.graph.Graph<QueryGraphNode, DBRelationship> jungGraph = new SparseMultigraph<QueryGraphNode, DBRelationship>();

	public QueryGraph(DBDatabase database, List<DBRow> allQueryTables, QueryOptions options) {
		addAndConnectToRelevant(database, allQueryTables, null, options);
	}

	public QueryGraph(DBDatabase database, List<DBRow> allQueryTables, List<BooleanExpression> expressions, QueryOptions options) {
		addAndConnectToRelevant(database, allQueryTables, expressions, options);
	}

	public QueryGraph clear() {
		nodes.clear();
		rows.clear();
		clearDisplayGraph();
		return this;
	}

	public final void addAndConnectToRelevant(DBDatabase database, List<DBRow> otherTables, List<BooleanExpression> expressions, QueryOptions options) {
		addAndConnectToRelevant(database, otherTables, expressions, options, true);
	}

	public final void addOptionalAndConnectToRelevant(DBDatabase database, List<DBRow> otherTables, List<BooleanExpression> expressions, QueryOptions options) {
		addAndConnectToRelevant(database, otherTables, expressions, options, false);
	}

	public final void addAndConnectToRelevant(DBDatabase database, List<DBRow> otherTables, List<BooleanExpression> expressions, QueryOptions options, boolean requiredTables) {

		List<DBRow> tablesAdded = new ArrayList<DBRow>();
		List<DBRow> tablesRemaining = new ArrayList<DBRow>();
		tablesRemaining.addAll(rows.values());
		tablesRemaining.addAll(otherTables);
		clearDisplayGraph();

		while (tablesRemaining.size() > 0) {
			DBRow table1 = tablesRemaining.get(0);
			Class<? extends DBRow> table1Class = table1.getClass();
			QueryGraphNode node1 = nodes.get(table1Class);
			if (node1 == null) {
				node1 = new QueryGraphNode(table1Class, requiredTables);
				addNodeToDisplayGraph(node1);
				nodes.put(table1Class, node1);
				rows.put(table1Class, table1);
			}
			for (DBRow table2 : tablesAdded) {
				if (!table1.getClass().equals(table2.getClass())) {
					if (table1.willBeConnectedTo(database, table2, options)) {
						Class<? extends DBRow> table2Class = table2.getClass();
						QueryGraphNode node2 = nodes.get(table2Class);
						node1.connectTable(table2Class);
						node2.connectTable(table1Class);
						addEdgesToDisplayGraph(database, table1, node1, table2, node2, options);
					}
				}
			}
			tablesAdded.add(table1);
			tablesRemaining.remove(table1);
		}

		for (BooleanExpression expr : expressions) {
			Set<DBRow> tables = expr.getTablesInvolved();
			for (DBRow table : tables) {
				Set<DBRow> tablesToConnectTo = new HashSet<DBRow>(tables);
				tablesToConnectTo.remove(table);
				final QueryGraphNode node1 = new QueryGraphNode(table.getClass());
				addNodeToDisplayGraph(node1);
				for (DBRow tab2 : tablesToConnectTo) {
					final QueryGraphNode node2 = new QueryGraphNode(tab2.getClass());
					addNodeToDisplayGraph(node2);
					addEdgeToDisplayGraph(table, node1, tab2, node2);
				}
			}
		}
	}

	private void addNodeToDisplayGraph(QueryGraphNode node1) {
		if (!jungGraph.containsVertex(node1)) {
			jungGraph.addVertex(node1);
		}
	}

	private void addEdgesToDisplayGraph(DBDatabase database, DBRow table1, QueryGraphNode node1, DBRow table2, QueryGraphNode node2, QueryOptions options) {
		for (DBRelationship fk : table1.getRelationshipsFromThisInstance(database, table2, options)) {
			if (!jungGraph.containsEdge(fk)) {
				jungGraph.addEdge(fk, node1, node2);
			}
		}
	}

	private void addEdgeToDisplayGraph(DBRow table1, QueryGraphNode node1, DBRow table2, QueryGraphNode node2) {
		DBRelationship fk = new DBRelationship(table1, table1.getPrimaryKey(), table2, table2.getPrimaryKey());

		if (!jungGraph.containsEdge(fk)) {
			jungGraph.addEdge(fk, node1, node2);
		}
	}

	public boolean willCreateCartesianJoin() { //willCreateCartesianJoin
		Set<DBRow> returnTables = new HashSet<DBRow>();
		Class<? extends DBRow> startFrom = nodes.keySet().iterator().next();
		returnTables.addAll(toList(startFrom));

		for (DBRow row : rows.values()) {
			if (!returnTables.contains(row)) {
				return true;
			}
		}
		return false;
	}

	public List<DBRow> toList(Class<? extends DBRow> startFrom) {
		HashSet<Class<? extends DBRow>> sortedTables = new LinkedHashSet<Class<? extends DBRow>>();
		List<Class<? extends DBRow>> addedTables = new ArrayList<Class<? extends DBRow>>();
		List<DBRow> returnTables = new ArrayList<DBRow>();
		QueryGraphNode nodeA = nodes.get(startFrom);
		sortedTables.add(nodeA.getTable());
		int sortedBeforeLoop = 0;
		while (sortedTables.size() > sortedBeforeLoop) {
			sortedBeforeLoop = sortedTables.size();
			addedTables.clear();
			for (Class<? extends DBRow> row : sortedTables) {
				nodeA = nodes.get(row);
				for (Class<? extends DBRow> table : nodeA.getConnectedTables()) {
					QueryGraphNode nodeToAdd = nodes.get(table);
					addedTables.add(nodeToAdd.getTable());
				}
			}
			sortedTables.addAll(addedTables);
		}
		for (Class<? extends DBRow> rowClass : sortedTables) {
			returnTables.add(rows.get(rowClass));
		}
		return returnTables;
	}

	public List<DBRow> toListIncludingCartesian(Class<? extends DBRow> startFrom) {
		Set<DBRow> returnTables = new HashSet<DBRow>();

		returnTables.addAll(toList(startFrom));
		boolean changed = true;

		while (changed) {
			for (DBRow row : rows.values()) {
				changed = false;
				if (!returnTables.contains(row)) {
					returnTables.addAll(toList(row.getClass()));
					changed = true;
				}
			}
		}
		final List<DBRow> returnList = new ArrayList<DBRow>();
		returnList.addAll(returnTables);
		return returnList;
	}

	public edu.uci.ics.jung.graph.Graph<QueryGraphNode, DBRelationship> getJungGraph() {
		return jungGraph;
	}

	private void clearDisplayGraph() {
		jungGraph = new SparseMultigraph<QueryGraphNode, DBRelationship>();
	}

}
