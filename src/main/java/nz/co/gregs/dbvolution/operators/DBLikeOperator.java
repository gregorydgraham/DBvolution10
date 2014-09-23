/*
 * Copyright 2013 gregorygraham.
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
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;

/**
 *
 * @author gregorygraham
 */
public class DBLikeOperator extends DBOperator {

	public static final long serialVersionUID = 1L;
//    private final QueryableDatatype firstValue;
	private StringExpression likeableValue;

	public DBLikeOperator(String likeableValue) {
		super();
		this.likeableValue = likeableValue == null ? null : new StringExpression(likeableValue);
	}

	public DBLikeOperator(StringExpression likeableValue) {
		super();
		this.likeableValue = likeableValue == null ? likeableValue : likeableValue.copy();
	}

	public DBLikeOperator() {
		super();
		this.firstValue = null;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDatabase db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression returnExpression = BooleanExpression.trueExpression();
		if (genericExpression instanceof StringExpression) {
			StringExpression strExpr = (StringExpression) genericExpression;
			returnExpression = strExpr.bracket().isLike(getLikeableValue());
		}
		if (invertOperator) {
			return returnExpression.not();
		} else {
			return returnExpression;
		}
	}

	@Override
	public DBLikeOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		DBLikeOperator op;
		op = new DBLikeOperator((StringExpression) typeAdaptor.convert(getLikeableValue()));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	/**
	 * @return the likeableValue
	 */
	protected StringExpression getLikeableValue() {
		return likeableValue;
	}
}
