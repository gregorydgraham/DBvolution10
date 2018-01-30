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
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Provides support for the abstract concept of updating rows to the previous
 * value of the updated columns.
 *
 * <p>
 * Used to provide revert actions for updates.
 * </p>
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBUpdateToPreviousValues extends DBUpdateSimpleTypes {

	private static final long serialVersionUID = 1l;
	
	DBUpdateToPreviousValues(DBRow row) {
		super(row);
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
	@Override
	protected String getSetClause(DBDatabase db, DBRow row) {
		DBDefinition defn = db.getDefinition();
		StringBuilder sql = new StringBuilder();
		List<PropertyWrapper> fields = row.getColumnPropertyWrappers();

		String separator = defn.getStartingSetSubClauseSeparator();
		for (PropertyWrapper field : fields) {
			if (field.isColumn()) {
				final QueryableDatatype<?> qdt = field.getQueryableDatatype();
				if (qdt.hasChanged()) {
					String previousSQLValue = qdt.getPreviousSQLValue(defn);
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
	 * @param db the target database
	 * @param row the row to be updated
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The WHERE clause of the UPDATE statement.
	 */
	@Override
	protected String getWhereClause(DBDatabase db, DBRow row) {
		DBDefinition defn = db.getDefinition();
		StringBuilder sqlString = new StringBuilder();
		List<QueryableDatatype<?>> primaryKeys = row.getPrimaryKeys();
		String separator = "";
		if (primaryKeys.size() > 0) {
			for (QueryableDatatype<?> pk : primaryKeys) {
				PropertyWrapper wrapper = row.getPropertyWrapperOf(pk);
				String pkValue = pk.toSQLString(defn);
				sqlString.append(separator).append(defn.formatColumnName(wrapper.columnName())).append(defn.getEqualsComparator()).append(pkValue);
				separator = defn.beginAndLine();
			}
		} else {
			for (PropertyWrapper prop : row.getColumnPropertyWrappers()) {
				QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				if (qdt.isNull()) {
					sqlString.append(separator).append(BooleanExpression.isNull(row.column(qdt)).toSQLString(defn));
				} else {
					sqlString.append(separator).append(prop.columnName()).append(defn.getEqualsComparator()).append(qdt.toSQLString(defn));
				}
				separator = defn.beginAndLine();
			}
		}
		return sqlString.toString();

	}
}
