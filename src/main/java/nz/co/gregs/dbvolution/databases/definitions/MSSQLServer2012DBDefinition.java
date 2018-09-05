/*
 * Copyright 2015 gregory.graham.
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
package nz.co.gregs.dbvolution.databases.definitions;

import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.query.QueryState;

/**
 * Extends and updates the MS SQLServer database definition to use features made
 * available by the 2012 version of MS SQLServer.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class MSSQLServer2012DBDefinition extends MSSQLServerDBDefinition {

	@Override
	public boolean supportsPagingNatively(QueryOptions options) {
		return true;
	}

	@Override
	public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return "";
	}

	@Override
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
		StringBuilder returnString = new StringBuilder();
		if (state.hasBeenOrdered()==false) {
			returnString.append(" ORDER BY 1 ");
		}
		if (options.getRowLimit() > 0) {
			returnString.append(" OFFSET ").append(options.getPageIndex() * options.getRowLimit()).append(" ROWS");
			returnString.append(" FETCH NEXT ").append(options.getRowLimit()).append(" ROWS ONLY ");
		}
		return returnString.toString();
	}

	@Override
	public String getChooseFunctionName() {
		return "CHOOSE";
	}

	@Override
	protected boolean supportsChooseNatively() {
		return true;
	}

	@Override
	public String doIfThenElseTransform(String booleanTest, String thenResult, String elseResult) {
		return " IIF( (" + booleanTest + "), (" + thenResult + "), (" + elseResult + "))";
	}

	@Override
	public int getNumericPrecision() {
		return 38;
	}

	@Override
	public boolean requiresOnClauseForAllJoins() {
		return true;
	}
}
