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

import java.io.Serializable;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;

/**
 *
 * @author Gregory Graham
 */
abstract public class DBOperator implements Serializable {

	private static final long serialVersionUID = 1L;

	Boolean invertOperator = false;
	Boolean includeNulls = false;
	private DBExpression firstValue;
	private DBExpression secondValue;
	private DBExpression thirdValue;
	private BooleanExpression expression;

	public DBOperator() {
		firstValue = null;
		secondValue = null;
		thirdValue = null;
	}

	public DBOperator(DBExpression first) {
		firstValue = first;
		secondValue = null;
		thirdValue = null;
	}

	public DBOperator(DBExpression first, DBExpression second) {
		firstValue = first;
		secondValue = second;
		thirdValue = null;
	}

	public DBOperator(DBExpression first, DBExpression second, DBExpression third) {
		firstValue = first;
		secondValue = second;
		thirdValue = third;
	}

	protected DBExpression getExpression() {
		return this.expression;
	}

	protected void setExpression(BooleanExpression operatorExpression) {
		this.expression = operatorExpression;
	}

	public void invertOperator(Boolean invertOperator) {
		this.invertOperator = invertOperator;
	}

	public void not() {
		invertOperator = true;
	}

	public void includeNulls() {
		includeNulls = true;
	}

//	@Override
//	public int hashCode() {
//		int hash = 7;
//		hash = 41 * hash + (this.invertOperator != null ? this.invertOperator.hashCode() : 0);
//		hash = 41 * hash + (this.includeNulls != null ? this.includeNulls.hashCode() : 0);
//		hash = 41 * hash + (this.firstValue != null ? this.firstValue.hashCode() : 0);
//		hash = 41 * hash + (this.secondValue != null ? this.secondValue.hashCode() : 0);
//		hash = 41 * hash + (this.thirdValue != null ? this.thirdValue.hashCode() : 0);
//		hash = 41 * hash + (this.expression != null ? this.expression.hashCode() : 0);
//		return hash;
//	}
	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object otherObject) {
		if (otherObject instanceof DBOperator) {
			DBOperator other = (DBOperator) otherObject;
			return this.getClass() == other.getClass()
					&& this.invertOperator.equals(other.invertOperator)
					&& this.includeNulls.equals(other.includeNulls)
					&& (getFirstValue() == null ? other.getFirstValue() == null : getFirstValue().equals(other.getFirstValue()))
					&& (getSecondValue() == null ? other.getSecondValue() == null : getSecondValue().equals(other.getSecondValue()))
					&& (getThirdValue() == null ? other.getThirdValue() == null : getThirdValue().equals(other.getThirdValue()));
		} else {
			return false;
		}
	}

	abstract public DBOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor);

	abstract public BooleanExpression generateWhereExpression(DBDatabase db, DBExpression column);

	/**
	 * @return the firstValue
	 */
	public DBExpression getFirstValue() {
		return firstValue;
	}

	/**
	 * @param firstValue the firstValue to set
	 */
	public void setFirstValue(DBExpression firstValue) {
		this.firstValue = firstValue;
	}

	/**
	 * @return the secondValue
	 */
	public DBExpression getSecondValue() {
		return secondValue;
	}

	/**
	 * @param secondValue the secondValue to set
	 */
	public void setSecondValue(DBExpression secondValue) {
		this.secondValue = secondValue;
	}

	/**
	 * @return the thirdValue
	 */
	public DBExpression getThirdValue() {
		return thirdValue;
	}

	/**
	 * @param thirdValue the thirdValue to set
	 */
	public void setThirdValue(DBExpression thirdValue) {
		this.thirdValue = thirdValue;
	}
}
