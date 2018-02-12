/*
 * Copyright 2014 Gregory Graham.
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
import java.util.HashSet;
import java.util.Set;

/**
 * Nodes (a.k.a. vertexes) of the {@link QueryGraph}
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class QueryGraphNode {

	private boolean requiredNode = true;

	private final DBRowClass table;
	private final Set<DBRowClass> connectedTables = new HashSet<DBRowClass>();

	/**
	 * Create a node for the supplied table.
	 *
	 * <p>
	 * The table is assumed to be a required/inner table.
	 *
	 * @param table
	 */
	public QueryGraphNode(DBRowClass table) {
		this.table = table;
	}

	/**
	 * Create a node for the supplied table, with TRUE if the table is a
	 * required/inner table or FALSE if it is optional/outer.
	 *
	 * @param table
	 * @param requiredTable
	 */
	public QueryGraphNode(DBRowClass table, boolean requiredTable) {
		this.table = table;
		requiredNode = requiredTable;
	}

	/**
	 * Return all connected tables known by this node.
	 *
	 * <p>
	 * Only includes DBRows/tables that have been connected to this node using {@link #connectTable(java.lang.Class)
	 * }.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a set of all classes used in this query
	 */
	public Set<DBRowClass> getConnectedTables() {
		return connectedTables;
	}

	/**
	 * Add a connection from this node to the specified table.
	 *
	 * <p>
	 * To retrieval all connected tables, use {@link #getConnectedTables() }.
	 *
	 * @param table
	 */
	public void connectTable(DBRowClass table) {
		connectedTables.add(table);
	}

	/**
	 * Retrieves the table that this node contains.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the table
	 */
	public DBRowClass getTable() {
		return table;
	}

	@Override
	public String toString() {
		return table.getSimpleName();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof QueryGraphNode) {
			QueryGraphNode otherNode = (QueryGraphNode) o;
			if (this.table.equals(otherNode.table)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 89 * hash + (this.table != null ? this.table.hashCode() : 0);
		return hash;
	}

	/**
	 * Specifies whether this node represents a required/inner table, or an
	 * optional/outer table.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the table is a required/inner table, otherwise FALSE.
	 */
	public boolean isRequiredNode() {
		return requiredNode;
	}
}
