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

import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Provides support for the abstract concept of updating rows without primary
 * keys.
 *
 * <p>
 * The best way to use this is by using {@link DBUpdate#getUpdates(nz.co.gregs.dbvolution.DBRow...)
 * } to automatically use this action.
 *
 * <p style="color: #F90;">Support DBvolution at <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @author Gregory Graham
 */
public class DBUpdateSimpleTypesUsingAllColumns extends DBUpdateSimpleTypes {

	DBUpdateSimpleTypesUsingAllColumns(DBRow row) {
		super(row);
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		DBRow row = getRow();
		DBDefinition defn = db.getDefinition();

		StringBuilder sql = new StringBuilder()
				.append(defn.beginUpdateLine())
				.append(defn.formatTableName(row))
				.append(defn.beginSetClause())
				.append(getSetClause(db, row))
				.append(defn.beginWhereClause())
				.append(defn.getWhereClauseBeginningCondition());
		for (PropertyWrapper prop : row.getColumnPropertyWrappers()) {
			QueryableDatatype<?> qdt = prop.getQueryableDatatype();
			if (qdt.isNull()) {
				sql.append(defn.beginAndLine())
						.append(BooleanExpression.isNull(row.column(qdt)).toSQLString(db));
//				DBIsNullOperator isNullOp = new DBIsNullOperator();
//				sql += isNullOp.generateWhereLine(db, prop.columnName());
			} else {
				sql.append(defn.beginWhereClauseLine())
						.append(prop.columnName())
						.append(defn.getEqualsComparator()) 
						.append(qdt.hasChanged() ? qdt.getPreviousSQLValue(db) : qdt.toSQLString(db));
			}
		}
		sql.append(defn.endDeleteLine());
		List<String> sqls = new ArrayList<>();
		sqls.add(sql.toString());
		return sqls;
	}
}
