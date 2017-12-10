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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.IntegerResult;
import nz.co.gregs.dbvolution.results.StringResult;

/**
 * Creates an operator that compares a column to a list of values using the IN
 * operator or similar.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBInOperator extends DBOperator {

	private static final long serialVersionUID = 1L;
	private final List<DBExpression> listOfPossibleValues = new ArrayList<>();
	private final List<StringResult> listOfPossibleStrings = new ArrayList<>();
	private final List<NumberResult> listOfPossibleNumbers = new ArrayList<>();
	private final ArrayList<IntegerResult> listOfPossibleIntegers = new ArrayList<>();
	private final List<DateResult> listOfPossibleDates = new ArrayList<>();

	/**
	 * Creates an operator that compares a column to a list of values using the IN
	 * operator or similar.
	 *
	 * @param listOfPossibleValues
	 */
	public DBInOperator(Collection<DBExpression> listOfPossibleValues) {
		super();
		for (DBExpression expr : listOfPossibleValues) {
			final DBExpression newExpr = expr == null ? null : expr.copy();
			this.listOfPossibleValues.add(newExpr);
			if (newExpr == null) {
				includeNulls = true;
			} else if (newExpr instanceof StringResult) {
				listOfPossibleStrings.add((StringResult) newExpr);
			} else if ((newExpr instanceof NumberResult)) {
				listOfPossibleNumbers.add((NumberResult) newExpr);
			} else if ((newExpr instanceof IntegerResult)) {
				listOfPossibleIntegers.add((IntegerResult) newExpr);
			} else if ((newExpr instanceof DateResult)) {
				listOfPossibleDates.add((DateResult) newExpr);
			}
		}
	}

	/**
	 * Default constructor
	 *
	 */
	protected DBInOperator() {
		super();
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object other) {

		if (super.equals(other) == false) {
			return false;
		} else {
			if (other instanceof DBInOperator) {
				DBInOperator otherIn = (DBInOperator) other;
				if (getListOfPossibleValues().size() != otherIn.getListOfPossibleValues().size()) {
					return false;
				} else {
					for (DBExpression qdt : getListOfPossibleValues()) {
						if (!otherIn.listOfPossibleValues.contains(qdt)) {
							return false;
						}
					}
				}
			} else {
				return false;
			}
			return true;
		}
	}

	@Override
	public DBInOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		ArrayList<DBExpression> list = new ArrayList<>();
		for (DBExpression item : getListOfPossibleValues()) {
			list.add(typeAdaptor.convert(item));
		}
		DBInOperator op = new DBInOperator(list);
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) {
		DBExpression genericExpression = column;
		BooleanExpression op = BooleanExpression.trueExpression();
		if (genericExpression instanceof StringExpression) {
			ArrayList<StringResult> listString = new ArrayList<>(getListOfPossibleStrings());
			if (this.includeNulls) {
				listString.add(null);
			}
			StringExpression stringExpression = (StringExpression) genericExpression;
			op = stringExpression.bracket().isIn(listString.toArray(new StringResult[]{}));
		} else if (genericExpression instanceof NumberExpression) {
			ArrayList<NumberResult> listNumbers = new ArrayList<>(getListOfPossibleNumbers());
			if (this.includeNulls) {
				listNumbers.add(null);
			}
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			op = numberExpression.isIn(listNumbers.toArray(new NumberResult[]{}));
		} else if (genericExpression instanceof IntegerExpression) {
			ArrayList<IntegerResult> listIntegers = new ArrayList<>(getListOfPossibleIntegers());
			if (this.includeNulls) {
				listIntegers.add(null);
			}
			IntegerExpression numberExpression = (IntegerExpression) genericExpression;
			op = numberExpression.isIn(listIntegers.toArray(new IntegerResult[]{}));
		} else if (genericExpression instanceof DateExpression) {
			ArrayList<DateResult> listDate = new ArrayList<>(getListOfPossibleDates());
			if (this.includeNulls) {
				listDate.add(null);
			}
			DateExpression dateExpression = (DateExpression) genericExpression;
			op = dateExpression.isIn(listDate.toArray(new DateResult[]{}));
		}
		return this.invertOperator ? op.not() : op;
	}

	/**
	 * List of supplied values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the listOfPossibleValues
	 */
	public List<DBExpression> getListOfPossibleValues() {
		return listOfPossibleValues;
	}

	/**
	 * List of strings derived, if any, from the supplied values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the listOfPossibleStrings
	 */
	public List<StringResult> getListOfPossibleStrings() {
		return listOfPossibleStrings;
	}

	/**
	 * List of numbers derived, if any, from the supplied values.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the listOfPossibleNumbers
	 */
	public List<NumberResult> getListOfPossibleNumbers() {
		return listOfPossibleNumbers;
	}

	/**
	 * List of numbers derived, if any, from the supplied values.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the listOfPossibleNumbers
	 */
	public List<IntegerResult> getListOfPossibleIntegers() {
		return listOfPossibleIntegers;
	}

	/**
	 * List of dates derived, if any, from the supplied values.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the listOfPossibleDates
	 */
	public List<DateResult> getListOfPossibleDates() {
		return listOfPossibleDates;
	}
}
