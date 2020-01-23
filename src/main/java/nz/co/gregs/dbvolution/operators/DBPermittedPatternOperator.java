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

import nz.co.gregs.dbvolution.expressions.StringExpression;

/**
 * Implements the LIKE operator for Strings
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBPermittedPatternOperator extends DBLikeOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements the LIKE operator for Strings
	 *
	 * @param likeableValue a pattern to be used with the LIKE operator
	 */
	public DBPermittedPatternOperator(String likeableValue) {
		super(StringExpression.value(likeableValue));
	}

	/**
	 * Implements the LIKE operator for Strings
	 *
	 * @param likeableValue a pattern to be used with the LIKE operator
	 */
	public DBPermittedPatternOperator(StringExpression likeableValue) {
		super(likeableValue);
	}

	/**
	 * Default constructor
	 *
	 */
	public DBPermittedPatternOperator() {
	}

}
