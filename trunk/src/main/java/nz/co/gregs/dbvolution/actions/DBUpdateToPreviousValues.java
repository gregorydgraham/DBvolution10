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

import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Provides support for the abstract concept of updating rows to the previous
 * value of the updated columns.
 *
 * <p>
 * Used to provide revert actions for updates.
 *
 * @author Gregory Graham
 */
public class DBUpdateToPreviousValues extends DBUpdateSimpleTypes {

	DBUpdateToPreviousValues(DBRow row) {
		super(row);
	}

	/**
	 * Creates the required SET clause of the UPDATE statement.
	 *
	 * @param db
	 * @param row
	 * @return The SET clause of the UPDATE statement.
	 */
	@Override
	protected String getSetClause(DBDatabase db, DBRow row) {
		DBDefinition defn = db.getDefinition();
		StringBuilder sql = new StringBuilder();
		List<PropertyWrapper> fields = row.getPropertyWrappers();

		String separator = defn.getStartingSetSubClauseSeparator();
		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				final QueryableDatatype qdt = field.getQueryableDatatype();
				if (qdt.hasChanged()) {
					String previousSQLValue = qdt.getPreviousSQLValue(db);
					if (previousSQLValue == null) {
						previousSQLValue = defn.getNull();
					}

					String columnName = field.columnName();
					sql.append(separator)
							.append(defn.formatColumnName(columnName))
							.append(defn.getEqualsComparator())
							.append(previousSQLValue);
					separator = defn.getSubsequentSetSubClauseSeparator();
				}
			}
		}
		return sql.toString();
	}

	/**
	 * Creates the WHERE clause of the UPDATE statement.
	 *
	 * @param db
	 * @param row
	 * @return The WHERE clause of the UPDATE statement.
	 */
	@Override
	protected String getWhereClause(DBDatabase db, DBRow row) {
		DBDefinition defn = db.getDefinition();
		QueryableDatatype primaryKey = row.getPrimaryKey();
		String pkCurrentValue = primaryKey.toSQLString(db);
		return defn.formatColumnName(row.getPrimaryKeyColumnName())
				+ defn.getEqualsComparator()
				+ pkCurrentValue;
	}
}
