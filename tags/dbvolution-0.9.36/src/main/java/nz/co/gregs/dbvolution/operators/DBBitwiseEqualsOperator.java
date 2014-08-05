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

package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.DBExpression;


public class DBBitwiseEqualsOperator extends DBEqualsOperator {

	public static final long serialVersionUID = 1L;

	public DBBitwiseEqualsOperator() {
	}

	public DBBitwiseEqualsOperator(DBExpression equalTo) {
		super(equalTo);
	}

	public DBBitwiseEqualsOperator(Object equalTo) {
		super(equalTo);
	}
	
	@Override
	public String generateWhereLine(DBDatabase db, String columnName) {
		DBDefinition defn = db.getDefinition();
		String whereLine;
		if ((firstValue instanceof QueryableDatatype) && ((QueryableDatatype) firstValue).isNull()) {
			DBIsNullOperator dbIsNullOperator = new DBIsNullOperator();
			dbIsNullOperator.invertOperator(this.invertOperator);
			whereLine = dbIsNullOperator.generateWhereLine(db, columnName);
		} else {
			whereLine = defn.convertBitsToInteger(columnName) + (invertOperator ? getInverse(defn) : getOperator(defn)) + defn.convertBitsToInteger(firstValue.toSQLString(db));
		}
		if (this.includeNulls) {
			DBIsNullOperator dbIsNullOperator = new DBIsNullOperator();
			dbIsNullOperator.invertOperator(this.invertOperator);
			return "(" + dbIsNullOperator.generateWhereLine(db, columnName) + (this.invertOperator?defn.beginAndLine():defn.beginOrLine()) + whereLine + ")";
		} else {
			return whereLine;
		}
	}
}
