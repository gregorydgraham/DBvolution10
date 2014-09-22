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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.expressions.StringResult;

/**
 *
 * @author gregorygraham
 */
public class DBInOperator extends DBOperator {

	public static final long serialVersionUID = 1L;
	protected final List<DBExpression> listOfPossibleValues = new ArrayList<DBExpression>();
	protected final List<StringResult> listOfPossibleStrings = new ArrayList<StringResult>();
	protected final ArrayList<NumberResult> listOfPossibleNumbers = new ArrayList<NumberResult>();
	protected final ArrayList<DateResult> listOfPossibleDates = new ArrayList<DateResult>();

	public DBInOperator(Collection<DBExpression> listOfPossibleValues) {
		super();
		for (DBExpression qdt : listOfPossibleValues) {
			final DBExpression newQDT = qdt == null ? null : qdt.copy();
			this.listOfPossibleValues.add(newQDT);
			if (newQDT == null) {
				listOfPossibleStrings.add(null);
				listOfPossibleNumbers.add(null);
				listOfPossibleDates.add(null);
			} else if (newQDT instanceof StringResult) {
				listOfPossibleStrings.add((StringResult) newQDT);
			} else if ((newQDT instanceof NumberResult)) {
				listOfPossibleNumbers.add((NumberResult) newQDT);
			} else if ((newQDT instanceof DateResult)) {
				listOfPossibleDates.add((DateResult) newQDT);
			}
		}
	}

	public DBInOperator() {
		super();
	}

	@Override
	public DBOperator getInverseOperator() {
		return this;
	}

	@Override
	public boolean equals(DBOperator other) {

		if (super.equals(other) == false) {
			return false;
		} else {
			if (other instanceof DBInOperator) {
				DBInOperator otherIn = (DBInOperator) other;
				if (listOfPossibleValues.size() != otherIn.listOfPossibleValues.size()) {
					return false;
				} else {
					for (DBExpression qdt : listOfPossibleValues) {
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
		ArrayList<DBExpression> list = new ArrayList<DBExpression>();
		for (DBExpression item : listOfPossibleValues) {
			list.add(typeAdaptor.convert(item));
		}
		DBInOperator op = new DBInOperator(list);
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
			op = stringExpression.bracket().isIn(listOfPossibleStrings.toArray(new StringResult[]{}));
		} else if (genericExpression instanceof NumberExpression) {
			NumberExpression numberExpression = (NumberExpression) genericExpression;
			op = numberExpression.isIn(listOfPossibleNumbers.toArray(new NumberResult[]{}));
		} else if (genericExpression instanceof DateExpression) {
			DateExpression dateExpression = (DateExpression) genericExpression;
			op = dateExpression.isIn(listOfPossibleDates.toArray(new DateResult[]{}));
		}
		return this.invertOperator ? op.not() : op;
	}
}
