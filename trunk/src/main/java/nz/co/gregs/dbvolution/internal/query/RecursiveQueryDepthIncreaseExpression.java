/*
 * Copyright 2014 gregory.graham.
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
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 *
 * @author gregory.graham
 */
public class RecursiveQueryDepthIncreaseExpression implements DBExpression {

	public RecursiveQueryDepthIncreaseExpression() {
	}

	@Override
	public DBNumber getQueryableDatatypeForExpressionValue() {
		return new DBNumber();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return db.getDefinition().getRecursiveQueryDepthColumnName() + "+1";
	}

	@Override
	public RecursiveQueryDepthIncreaseExpression copy() {
		return this;
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		return new HashSet<DBRow>();
	}
	
}
