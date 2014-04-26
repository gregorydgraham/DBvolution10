/*
 * Copyright 2014 gregorygraham.
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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRow;

/**
 *
 * @author gregorygraham
 */
public class QueryGraphNode {
	private boolean requiredNode = true;

	private final Class<? extends DBRow> table;
	private final Set<Class<? extends DBRow>> connectedTables = new HashSet<Class<? extends DBRow>>();

	public QueryGraphNode(Class<? extends DBRow> table) {
		this.table = table;
	}

	public QueryGraphNode(Class<? extends DBRow> table, boolean requiredTable) {
		this.table = table;
		requiredNode = requiredTable;
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
	
	public boolean isRequiredNode() {
		return requiredNode;
	}
}
