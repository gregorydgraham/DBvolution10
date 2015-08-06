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
package nz.co.gregs.dbvolution.databases.definitions;

import nz.co.gregs.dbvolution.databases.Oracle12DB;
import nz.co.gregs.dbvolution.query.QueryOptions;

/**
 * Defines the features of the Oracle 12 database that differ from the standard
 * database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link Oracle12DB} instances,
 * and you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class Oracle12DBDefinition extends OracleSpatialDBDefinition {

	@Override
	public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		int rowLimit = options.getRowLimit();
		Integer pageNumber = options.getPageIndex();
		if (rowLimit < 1) {
			return "";
		} else {
			long offset = pageNumber * rowLimit;

			return " OFFSET " + offset + " ROWS FETCH NEXT " + rowLimit + " ROWS ONLY ";
		}
	}
}
