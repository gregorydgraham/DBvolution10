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
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanArrayExpression;
import nz.co.gregs.dbvolution.results.BooleanArrayResult;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.results.BooleanResult;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.expressions.spatial2D.Polygon2DExpression;
import nz.co.gregs.dbvolution.results.Polygon2DResult;
import nz.co.gregs.dbvolution.expressions.DateRepeatExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.results.DateRepeatResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.StringResult;

/**
 * Implements the EQUALS operator.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBEqualsOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements the EQUALS operator.
	 *
	 * @param equalTo
	 */
	@SuppressFBWarnings(
			value = "NP_LOAD_OF_KNOWN_NULL_VALUE",
			justification = "Null is a valid value in databases")
	public DBEqualsOperator(DBExpression equalTo) {
		super(equalTo == null ? equalTo : equalTo.copy());
	}

	/**
	 * Implements the EQUALS operator.
	 *
	 * @param equalTo
	 */
	public DBEqualsOperator(Object equalTo) {
		super(QueryableDatatype.getQueryableDatatypeForObject(equalTo));
	}

	@Override
	public DBEqualsOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		DBEqualsOperator op = new DBEqualsOperator(typeAdaptor.convert(getFirstValue()));
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	@SuppressWarnings("unchecked")
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression op = BooleanExpression.trueExpression();
		if (genericExpression instanceof EqualComparable) {
			try {
				EqualComparable<Object, DBExpression> columnEqual = (EqualComparable<Object, DBExpression>) genericExpression;
				if (invertOperator) {
					op = columnEqual.isNot(getFirstValue());
				} else {
					op = columnEqual.is(getFirstValue());
				}
				return op;
			} catch (Exception exp) {
				if (genericExpression instanceof StringExpression) {
					StringExpression stringExpression = (StringExpression) genericExpression;
					if ((getFirstValue() instanceof StringResult) || getFirstValue() == null) {
						op = stringExpression.bracket().is((StringResult) getFirstValue());
					} else if (getFirstValue() instanceof NumberResult) {
						op = stringExpression.bracket().is(new NumberExpression((NumberResult) getFirstValue()).stringResult());
					} else if (getFirstValue() instanceof IntegerResult) {
						op = stringExpression.bracket().is(new IntegerExpression((IntegerResult) getFirstValue()).stringResult());
					} else {
						throw new nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes(db, genericExpression, getFirstValue());
					}
				} else if ((genericExpression instanceof NumberExpression) && ((getFirstValue() instanceof NumberResult) || getFirstValue() == null)) {
					NumberExpression numberExpression = (NumberExpression) genericExpression;
					op = numberExpression.is((NumberResult) getFirstValue());
				} else if ((genericExpression instanceof NumberExpression) && ((getFirstValue() instanceof IntegerResult) || getFirstValue() == null)) {
					NumberExpression numberExpression = (NumberExpression) genericExpression;
					op = numberExpression.is(new IntegerExpression((IntegerResult) getFirstValue()).numberResult());
				} else if ((genericExpression instanceof IntegerExpression) && ((getFirstValue() instanceof IntegerResult) || getFirstValue() == null)) {
					IntegerExpression integerExpression = (IntegerExpression) genericExpression;
					op = integerExpression.is((IntegerResult) getFirstValue());
				} else if ((genericExpression instanceof IntegerExpression) && ((getFirstValue() instanceof NumberResult) || getFirstValue() == null)) {
					IntegerExpression integerExpression = (IntegerExpression) genericExpression;
					op = integerExpression.numberResult().is((NumberResult) getFirstValue());
				} else if ((genericExpression instanceof DateExpression) && ((getFirstValue() instanceof DateResult) || getFirstValue() == null)) {
					DateExpression dateExpression = (DateExpression) genericExpression;
					op = dateExpression.is((DateResult) getFirstValue());
				} else if ((genericExpression instanceof BooleanExpression) && ((getFirstValue() instanceof BooleanResult) || getFirstValue() == null)) {
					BooleanExpression boolExpr = (BooleanExpression) genericExpression;
					op = boolExpr.is((BooleanResult) getFirstValue());
				} else if ((genericExpression instanceof BooleanArrayExpression) && ((getFirstValue() instanceof BooleanArrayResult) || getFirstValue() == null)) {
					BooleanArrayExpression boolExpr = (BooleanArrayExpression) genericExpression;
					op = boolExpr.is((BooleanArrayResult) getFirstValue());
				} else if ((genericExpression instanceof DateRepeatExpression) && ((getFirstValue() instanceof DateRepeatResult) || getFirstValue() == null)) {
					DateRepeatExpression intervalExpr = (DateRepeatExpression) genericExpression;
					op = intervalExpr.is((DateRepeatResult) getFirstValue());
				} else if ((genericExpression instanceof Polygon2DExpression) && ((getFirstValue() instanceof Polygon2DResult) || getFirstValue() == null)) {
					Polygon2DExpression intervalExpr = (Polygon2DExpression) genericExpression;
					op = intervalExpr.is((Polygon2DResult) getFirstValue());
				} else {
					throw new nz.co.gregs.dbvolution.exceptions.ComparisonBetweenTwoDissimilarTypes(db, genericExpression, getFirstValue());
				}
				return this.invertOperator ? op.not() : op;
			}
		} else {
			throw new nz.co.gregs.dbvolution.exceptions.IncomparableTypeUsedInComparison(db, genericExpression);
		}
	}
}
