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
 * Implements a case-insensitive version of the LIKE operator for Strings
 *
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBPermittedPatternIgnoreCaseOperator extends DBLikeCaseInsensitiveOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements a case-insensitive version of the LIKE operator for Strings
	 *
	 *
	 * @param likeableValue
	 */
	public DBPermittedPatternIgnoreCaseOperator(String likeableValue) {
		super(new StringExpression(likeableValue));
	}

	/**
	 * Implements a case-insensitive version of the LIKE operator for Strings
	 *
	 *
	 * @param likeableValue
	 */
	public DBPermittedPatternIgnoreCaseOperator(StringExpression likeableValue) {
		super(likeableValue);
	}

	/**
	 * Default constructor
	 */
	public DBPermittedPatternIgnoreCaseOperator() {
	}
}
