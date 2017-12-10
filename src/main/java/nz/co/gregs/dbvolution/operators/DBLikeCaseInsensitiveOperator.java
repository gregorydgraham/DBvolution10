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

import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;

/**
 * Implements LIKE for all types that support it, but with case-insensitivity.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBLikeCaseInsensitiveOperator extends DBLikeOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements LIKE for all types that support it, but with case-insensitivity.
	 *
	 * @param likeableValue
	 */
	public DBLikeCaseInsensitiveOperator(String likeableValue) {
		super(likeableValue);
	}

	/**
	 * Implements LIKE for all types that support it, but with case-insensitivity.
	 *
	 * @param likeableValue
	 */
	public DBLikeCaseInsensitiveOperator(StringExpression likeableValue) {
		super(likeableValue);
	}

	/**
	 * Default constructor
	 *
	 */
	protected DBLikeCaseInsensitiveOperator() {
		super();
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) {
		DBExpression genericExpression = column;
		if (genericExpression instanceof StringExpression) {
			StringExpression strExpr = (StringExpression) genericExpression;
			return strExpr.bracket().isLikeIgnoreCase(getLikeableValue());
		}
		return BooleanExpression.trueExpression();
	}
}
