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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DBStatement;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Provides support for the abstract concept of updating rows with standard
 * columns.
 *
 * <p>
 * The best way to use this is by using {@link DBUpdate#getUpdates(nz.co.gregs.dbvolution.DBRow...)
 * } to automatically use this action.
 *
 * @author Gregory Graham
 */
public class DBUpdateSimpleTypes extends DBUpdate {

	private static final long serialVersionUID = 1l;
	protected final DBRow originalRow;

	DBUpdateSimpleTypes(DBRow row) {
		super(row);
		originalRow = row;
	}

	@Override
	protected DBActionList execute(DBDatabase db) throws SQLException {
		DBRow table = originalRow;
		DBActionList actions = new DBActionList(new DBUpdateSimpleTypes(table));
		try (DBStatement statement = db.getDBStatement()) {
			for (String sql : getSQLStatements(db)) {
				statement.execute("Update row", QueryIntention.UPDATE_ROW, sql);
			}
		}
		refetchIfClusterRequires(db, originalRow);
		return actions;
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		DBRow table = getRow();
		List<String> sqls = new ArrayList<>();
		DBDefinition defn = db.getDefinition();

		String sql = defn.beginUpdateLine()
				+ defn.formatTableName(table)
				+ defn.beginSetClause()
				+ getSetClause(db, table)
				+ defn.beginWhereClause()
				+ getWhereClause(db, table)
				+ defn.endDeleteLine();
		sqls.add(sql);
		return sqls;
	}

	/**
	 * Creates the required SET clause of the UPDATE statement.
	 *
	 * @param db the target database
	 * @param row the row to be updated
	 * @return The SET clause of the UPDATE statement.
	 */
	protected String getSetClause(DBDatabase db, DBRow row) {
		DBDefinition defn = db.getDefinition();
		StringBuilder sql = new StringBuilder();
		var fields = row.getColumnPropertyWrappers();

		String separator = defn.getStartingSetSubClauseSeparator();
		for (var field : fields) {
			if (field.isColumn()) {
				final QueryableDatatype<?> qdt = field.getQueryableDatatype();
				if (qdt != null) {
					if (!(qdt instanceof DBLargeObject)) {
						if (qdt.hasChanged()) {
							String columnName = field.columnName();
							sql.append(separator)
									.append(defn.formatColumnName(columnName))
									.append(defn.getEqualsComparator())
									.append(qdt
											.toSQLString(defn));
							separator = defn.getSubsequentSetSubClauseSeparator();
						} else if (qdt.hasDefaultUpdateValue()) {
							String columnName = field.columnName();
							sql.append(separator)
									.append(defn.formatColumnName(columnName))
									.append(defn.getEqualsComparator())
									.append(qdt.getDefaultUpdateValueSQLString(defn));
							separator = defn.getSubsequentSetSubClauseSeparator();
						}
					}
				}
			}
		}
		return sql.toString();
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList dbActionList = new DBActionList();
		dbActionList.add(new DBUpdateToPreviousValues(this.getRow()));
		return dbActionList;
	}

	/**
	 * Creates the WHERE clause of the UPDATE statement.
	 *
	 * @param db the target database
	 * @param row the row to be updated
	 * @return The WHERE clause of the UPDATE statement.
	 */
	protected String getWhereClause(DBDatabase db, DBRow row) {
		return getPrimaryKeySQL(db, row);
	}
}
