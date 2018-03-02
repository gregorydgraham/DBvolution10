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
package nz.co.gregs.dbvolution.internal.query;

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBRecursiveQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.expressions.NumberExpression;

/**
 * Creates a depth expression for the {@link DBRecursiveQuery} query.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class RecursiveQueryDepthIncreaseExpression extends NumberExpression {

	private static final long serialVersionUID = 1l;

	/**
	 * Creates a depth expression for the {@link DBRecursiveQuery} query.
	 *
	 */
	public RecursiveQueryDepthIncreaseExpression() {
	}

	@Override
	public DBNumber getQueryableDatatypeForExpressionValue() {
		return new DBNumber();
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return db.getRecursiveQueryDepthColumnName() + "+1";
	}

	@Override
	public RecursiveQueryDepthIncreaseExpression copy() {
		return (RecursiveQueryDepthIncreaseExpression) super.copy();
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<DBRow>();
	}

	@Override
	public boolean isPurelyFunctional() {
		return false;
	}
}
