/*
 * Copyright 2014 Gregory Graham.
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

import nz.co.gregs.dbvolution.expressions.BooleanExpression;

/**
 * Implements the EQUALS operator for DBBooleans
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBBooleanPermittedValuesOperator extends DBPermittedValuesOperator<Boolean> {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements the EQUALS operator for DBBooleans.
	 *
	 * @param permitted the boolean value that is permitted
	 */
	@SuppressWarnings("unchecked")
	public DBBooleanPermittedValuesOperator(Boolean permitted) {
		BooleanExpression expr;
		if (permitted == null) {
			operator = new DBIsNullOperator();
		} else {
			expr = BooleanExpression.value(permitted);
			operator = new DBBitwiseEqualsOperator(expr);
		}
		operator.includeNulls = this.includeNulls;
	}
}
