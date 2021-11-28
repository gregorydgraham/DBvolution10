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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import static nz.co.gregs.dbvolution.expressions.AnyExpression.value;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;

/**
 * Implements LIKE for all types that support it.
 *
 * @author Gregory Graham
 */
public class DBLikeOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements LIKE for all types that support it.
	 *
	 * @param likeableValue the value to compare the database entries with
	 */
	public DBLikeOperator(String likeableValue) {
		super(likeableValue == null ? null : value(likeableValue));
	}

	/**
	 * Implements LIKE for all types that support it.
	 *
	 * @param likeableValue the value to compare the database entries with
	 */
	@SuppressFBWarnings(
			value = "NP_LOAD_OF_KNOWN_NULL_VALUE",
			justification = "Null is a valid value in databases")
	public DBLikeOperator(StringExpression likeableValue) {
		super(likeableValue == null ? likeableValue : likeableValue.copy());
	}

	/**
	 * Default constructor
	 *
	 */
	public DBLikeOperator() {
		super();
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) {
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
		return (StringExpression) getFirstValue();
	}
}
