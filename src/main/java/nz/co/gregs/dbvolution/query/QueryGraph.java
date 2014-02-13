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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;

/**
 *
 * @author gregory.graham
 */
public class QueryGraph {

    final Map<Class<? extends DBRow>, QueryGraphNode> nodes = new LinkedHashMap<Class<? extends DBRow>, QueryGraphNode>();
    final Map<Class<? extends DBRow>, DBRow> rows = new LinkedHashMap<Class<? extends DBRow>, DBRow>();
    final private Object mutex = new Object();

    public QueryGraph(DBDatabase database, List<DBRow> allQueryTables, QueryOptions options) {
        addAndConnectToRelevant(database, allQueryTables, options);
    }

    private void addAndConnectToRelevant(DBDatabase database, List<DBRow> otherTables, QueryOptions options) {

        List<DBRow> tablesAdded = new ArrayList<DBRow>();
        List<DBRow> tablesRemaining = new ArrayList<DBRow>();
        tablesRemaining.addAll(otherTables);

        while (tablesRemaining.size() > 0) {
            DBRow table1 = tablesRemaining.get(0);
            Class<? extends DBRow> table1Class = table1.getClass();
            QueryGraphNode node1 = nodes.get(table1Class);
            if (node1 == null) {
                node1 = new QueryGraphNode(table1Class);
                nodes.put(table1Class, node1);
                rows.put(table1Class, table1);
            }
            for (DBRow table2 : tablesAdded) {
                if (!table1.getClass().equals(table2.getClass())) {
                    if (table1.willBeConnectedTo(database, table2, options)) {
                        Class<? extends DBRow> table2Class = table2.getClass();
                        QueryGraphNode node2 = nodes.get(table2Class);
                        if (node2 == null) {
                            node2 = new QueryGraphNode(table2Class);
                            nodes.put(table2Class, node2);
                            rows.put(table2Class, table2);
                        }
                        node1.connectTable(table2Class);
                        node2.connectTable(table1Class);
                    }
                }
            }
            tablesAdded.add(table1);
            tablesRemaining.remove(table1);
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

    private class QueryGraphNode {

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
