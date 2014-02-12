/*
 * Copyright 2013 gregory.graham.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;

/**
 *
 * @author gregory.graham
 */
public class QueryGraph {

    final Map<Class<? extends DBRow>, QueryGraphNode> nodes = new HashMap<Class<? extends DBRow>, QueryGraphNode>();
    final private Object mutex = new Object();

    public void add(Class<? extends DBRow> table1, Class<? extends DBRow> table2) {
        QueryGraphNode node1 = nodes.get(table1);
        if (node1 == null) {
            node1 = new QueryGraphNode(table1);
            nodes.put(table1, node1);
        }

        QueryGraphNode node2 = nodes.get(table2);
        if (node2 == null) {
            node2 = new QueryGraphNode(table2);
            nodes.put(table2, node2);
        }
        node1.connectTable(table2);
        node2.connectTable(table1);

    }

    public boolean hasDisconnectedSubgraph() {
        boolean disconnected = false;
        QueryGraph fullGraph = new QueryGraph();
        synchronized (mutex) {
            fullGraph.addAll(this);
        }
        if (!fullGraph.nodes.isEmpty()) {
            QueryGraphNode nodeA;
            QueryGraph testGraph = new QueryGraph();
            List<QueryGraphNode> added;
            List<QueryGraphNode> addedNext;
            int previousSize;

            nodeA = fullGraph.nodes.values().toArray(new QueryGraphNode[]{})[0];
            previousSize = testGraph.nodes.size();
            added = testGraph.addConnected(nodeA, nodes);

            while (testGraph.nodes.size() > previousSize) {
                previousSize = testGraph.nodes.size();
                addedNext = new ArrayList<QueryGraphNode>();
                for (QueryGraphNode node : added) {
                    addedNext.addAll(testGraph.addConnected(node, nodes));
                }
                added = addedNext;
            }

            if (testGraph.nodes.size() != fullGraph.nodes.size()) {
                disconnected = true;
            }
        }
        return disconnected;
    }

    private List<QueryGraphNode> addConnected(QueryGraphNode nodeA, Map<Class<? extends DBRow>, QueryGraphNode> nodes) {
        List<QueryGraphNode> addedTables = new ArrayList<QueryGraphNode>();
        for (Class<? extends DBRow> table : nodeA.getConnectedTables()) {
            QueryGraphNode nodeToAdd = nodes.get(table);
            Class<? extends DBRow> tableToAdd = nodeToAdd.getTable();
            add(nodeA.getTable(), tableToAdd);
            addedTables.add(nodeToAdd);
        }
        return addedTables;
    }

    public void add(Class<? extends DBRow> table) {
        QueryGraphNode node = nodes.get(table);
        if (node == null) {
            node = new QueryGraphNode(table);
            nodes.put(table, node);
        }
    }

    private void addAll(QueryGraph otherGraph) {
        for (Map.Entry<Class<? extends DBRow>, QueryGraphNode> entry : otherGraph.nodes.entrySet()) {
            this.nodes.put(entry.getKey(), new QueryGraphNode(entry.getValue()));
        }
    }

    public List<Class<? extends DBRow>> toList(Class<? extends DBRow> startFrom) {
        List<Class<? extends DBRow>> sortedTables = new ArrayList<Class<? extends DBRow>>();
        List<Class<? extends DBRow>> addedTables = new ArrayList<Class<? extends DBRow>>();
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
        return sortedTables;
    }

    public class QueryGraphNode {

        private final Class<? extends DBRow> table;
        private final Set<Class<? extends DBRow>> connectedTables = new HashSet<Class<? extends DBRow>>();

        public QueryGraphNode(Class<? extends DBRow> table) {
            this.table = table;
        }

        private QueryGraphNode(QueryGraphNode value) {
            this.table = value.table;
            this.connectedTables.addAll(value.connectedTables);
        }

        public Set<Class<? extends DBRow>> getConnectedTables() {
            return connectedTables;
        }

        public void connectTable(Class<? extends DBRow> table) {
            connectedTables.add(table);
        }

        /**
         * @return the table
         */
        public Class<? extends DBRow> getTable() {
            return table;
        }
    }
}
