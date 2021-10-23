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
package nz.co.gregs.dbvolution.expressions;

import nz.co.gregs.dbvolution.results.LargeObjectResult;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.AnyComparable;
import nz.co.gregs.dbvolution.results.AnyResult;

/**
 * LargeObjectExpression exposes database expressions for manipulating BLOBs,
 * CLOBs, and JavaObjects.
 *
 * @author gregorygraham
 * @param <BASETYPE> the fundamental Java type used with this LargeObjectExpression
 * @param <RESULT> the Result class used to represent this kind of LargeObjectExpression
 * @param <QDT> the QDT used to represent this values of this LargeObjectExpression
 */
public abstract class LargeObjectExpression<BASETYPE extends Object, RESULT extends AnyResult<BASETYPE>, QDT extends QueryableDatatype<BASETYPE>>
		extends AnyExpression<BASETYPE, RESULT, QDT>
		implements LargeObjectResult<BASETYPE>, AnyComparable<BASETYPE, RESULT> {

	private final static long serialVersionUID = 1l;

	/**
	 * Default Constructor.
	 */
	protected LargeObjectExpression() {
		super();
	}

	/**
	 * Wraps the LargeObjectResult with a LargeObjectExpression to allow further
	 * processing.
	 *
	 * @param originalBlob	originalBlob
	 */
	public LargeObjectExpression(RESULT originalBlob) {
		super(originalBlob);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return getInnerResult().toSQLString(db);
	}

	@Override
	public String createSQLForFromClause(DBDatabase database) {
		throw new UnsupportedOperationException("LargeObjectExpresssion does not support createSQLForFromClause(DBDatabase) yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean isComplexExpression() {
		return false;
	}

	@Override
	public boolean isPurelyFunctional() {
		return getTablesInvolved().isEmpty();
	}
}
