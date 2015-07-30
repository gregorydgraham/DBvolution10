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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.StringResult;

public class DBEqualsIgnoreCaseOperator extends DBEqualsOperator {

	private static final long serialVersionUID = 1L;

	public DBEqualsIgnoreCaseOperator() {
		super();
	}

	public DBEqualsIgnoreCaseOperator(DBExpression equalTo) {
		super(equalTo);
	}

//    @Override
//    public String generateWhereLine(DBDatabase db, String columnName) {
////        firstValue.setDatabase(database);
//        DBDefinition defn = db.getDefinition();
//        if (firstValue.toSQLString(db).equals(defn.getNull())) {
//            DBIsNullOperator dbIsNullOperator = new DBIsNullOperator();
//            return dbIsNullOperator.generateWhereLine(db, columnName);
//        }
//        return defn.toLowerCase(columnName) + (invertOperator ? getInverse(defn) : getOperator(defn)) + defn.toLowerCase(firstValue.toSQLString(db)) + " ";
//    }
//    @Override
//    public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
//        DBDefinition defn = database.getDefinition();
//        return defn.toLowerCase(columnName) + (invertOperator ? getInverse(defn) : getOperator(defn)) + defn.toLowerCase(otherColumnName);
//    }
	@Override
	public DBEqualsIgnoreCaseOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		DBEqualsIgnoreCaseOperator op = new DBEqualsIgnoreCaseOperator(typeAdaptor.convert(getFirstValue()));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDatabase db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression op = BooleanExpression.trueExpression();
		if (genericExpression instanceof StringExpression) {
			StringExpression stringExpression = (StringExpression) genericExpression;
			op = stringExpression.bracket().isIgnoreCase((StringResult) getFirstValue());
		} else if (genericExpression instanceof NumberExpression) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			op = numberExpression.is((NumberResult) getFirstValue());
		} else if (genericExpression instanceof DateExpression) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			op = dateExpression.is((DateResult) getFirstValue());
		}
		return this.invertOperator ? op.not() : op;
	}
}
