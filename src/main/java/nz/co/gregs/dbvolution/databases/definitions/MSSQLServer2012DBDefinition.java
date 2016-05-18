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

import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 * Extends and updates the MS SQLServer database definition to use features made
 * available by the 2012 version of MS SQLServer.
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
	public String getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		String returnString = "";
		if (options.getSortColumns().length == 0) {
			returnString = " order by 1";
		}
		returnString += " OFFSET " + options.getPageIndex() + " ROWS FETCH NEXT " + options.getRowLimit() + " ROWS ONLY ";
		return returnString;
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
		return " IFF( (" + booleanTest + "), (" + thenResult + "), (" + elseResult + "))";
	}

}
