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
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 * Provides a framework on which to build operators that are combinations of
 * operators.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
abstract class DBMetaOperator extends DBOperator {

	private static final long serialVersionUID = 1L;

	protected DBOperator operator;

	@Override
	public DBOperator copyAndAdapt(QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor typeAdaptor) {
		return operator.copyAndAdapt(typeAdaptor);
	}

	@Override
	public BooleanExpression generateWhereExpression(DBDefinition db, DBExpression column) {
		return operator.generateWhereExpression(db, column);
	}

	@Override
	public void invertOperator(Boolean invertOperator) {
		operator.invertOperator(invertOperator);
	}

	@Override
	public void not() {
		operator.not();
	}
}
