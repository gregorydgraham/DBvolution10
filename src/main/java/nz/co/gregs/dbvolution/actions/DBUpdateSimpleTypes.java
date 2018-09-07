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
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Provides support for the abstract concept of updating rows with standard
 * columns.
 *
 * <p>
 * The best way to use this is by using {@link DBUpdate#getUpdates(nz.co.gregs.dbvolution.DBRow...)
 * } to automatically use this action.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBUpdateSimpleTypes extends DBUpdate {

	private static final long serialVersionUID = 1l;
	
	DBUpdateSimpleTypes(DBRow row) {
		super(row);
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {
		DBRow table = getRow();
		DBActionList actions = new DBActionList(new DBUpdateSimpleTypes(table));
		try (DBStatement statement = db.getDBStatement()) {
			for (String sql : getSQLStatements(db)) {
				statement.execute(sql);
			}
		}
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The SET clause of the UPDATE statement.
	 */
	protected String getSetClause(DBDatabase db, DBRow row) {
		DBDefinition defn = db.getDefinition();
		StringBuilder sql = new StringBuilder();
		List<PropertyWrapper> fields = row.getColumnPropertyWrappers();

		String separator = defn.getStartingSetSubClauseSeparator();
		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				final QueryableDatatype<?> qdt = field.getQueryableDatatype();
				if (qdt.hasChanged()) {
					String columnName = field.columnName();
					sql.append(separator)
							.append(defn.formatColumnName(columnName))
							.append(defn.getEqualsComparator())
							.append(qdt
									.toSQLString(db.getDefinition()));
					separator = defn.getSubsequentSetSubClauseSeparator();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The WHERE clause of the UPDATE statement.
	 */
	protected String getWhereClause(DBDatabase db, DBRow row) {
		return getPrimaryKeySQL(db, row);
	}

//	@Override
//	protected DBActionList getActions() {
//		return new DBActionList(new DBUpdateSimpleTypes(getRow()));
//	}
}
