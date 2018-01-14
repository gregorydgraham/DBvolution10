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
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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

	/**
	 * Create a DBOperator with all NULL values.
	 *
	 */
	public DBOperator() {
		firstValue = null;
		secondValue = null;
		thirdValue = null;
	}

	/**
	 * Create a DBOperator with the first parameter specified.
	 *
	 * @param first the first parameter of the operator.
	 */
	public DBOperator(DBExpression first) {
		firstValue = first;
		secondValue = null;
		thirdValue = null;
	}

	/**
	 * Create a DBOperator with first and second parameters specified.
	 *
	 * @param first the first parameter of the operator.
	 * @param second the second parameter of the operator.
	 */
	public DBOperator(DBExpression first, DBExpression second) {
		firstValue = first;
		secondValue = second;
		thirdValue = null;
	}

	/**
	 * Create a DBOperator with first, second, and third parameters specified.
	 *
	 * @param first the first parameter of the operator.
	 * @param second the second parameter of the operator.
	 * @param third the third expression of the operator.
	 */
	public DBOperator(DBExpression first, DBExpression second, DBExpression third) {
		firstValue = first;
		secondValue = second;
		thirdValue = third;
	}

	/**
	 * Make this operator an exclusive rather than inclusive comparison.
	 *
	 * <p>
	 * Basically switches the operator from, for instance, "==" to "!=".
	 *
	 * @param invertOperator
	 */
	public void invertOperator(Boolean invertOperator) {
		this.invertOperator = invertOperator;
	}

	/**
	 * Make this operator an exclusive rather than inclusive comparison.
	 *
	 * <p>
	 * Basically switches the operator from, for instance, "==" to "!=".
	 *
	 */
	public void not() {
		invertOperator = true;
	}

	/**
	 * Makes this operator treat NULL values as if they match the operator.
	 *
	 * <p>
	 * Basically this means an equals operation becomes an (equals or null)
	 * operation.
	 *
	 */
	public void includeNulls() {
		includeNulls = true;
	}

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

	/**
	 * Adds TypeAdaptor support to DBOperator.
	 *
	 * @param typeAdaptor
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the type adapted operator
	 */
	abstract public DBOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor);

	/**
	 * Create the expression to be used in the query generation.
	 *
	 * @param db
	 * @param column
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a boolean expression
	 */
	abstract public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column);

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
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
