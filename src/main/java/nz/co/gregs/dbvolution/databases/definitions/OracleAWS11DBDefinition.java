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

import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.OracleAWS11DB;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.query.QueryState;

/**
 * Defines the features of the Amazon's RDS Oracle 11 database that differ from
 * the standard database.
 *
 * <p>
 * This DBDefinition is automatically included in {@link OracleAWS11DB}
 * instances, and you should not need to use it directly.
 *
 * @author Gregory Graham
 */
public class OracleAWS11DBDefinition extends OracleAWSDBDefinition {

	public static final long serialVersionUID = 1L;
	
	@Override
	public String getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return " /*+ FIRST_ROWS(" + options.getRowLimit() + ") */ ";
	}

	@Override
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
		return "";
	}

//	@Override
//	public boolean supportsPagingNatively(QueryOptions options) {
//		return true;
//	}
	@Override
	public String getColumnAutoIncrementSuffix() {
		return "";
	}

	@Override
	public boolean prefersTriggerBasedIdentities() {
		return true;
	}

	@Override
	public List<String> getSQLToDropAnyAssociatedDatabaseObjects(DBRow tableRow) {
		ArrayList<String> result = new ArrayList<>(0);
		
		if (tableRow.getPrimaryKeys() != null) {
			final String formattedTableName = formatTableName(tableRow);
			final List<String> primaryKeyColumnNames = tableRow.getPrimaryKeyColumnNames();
			for (String primaryKeyColumnName : primaryKeyColumnNames) {
				String sql = getSQLToDropSequence(primaryKeyColumnName, formattedTableName);
				result.add(sql);
			}
		}
		return result;
	}

	protected String getSQLToDropSequence(String primaryKeyColumnName, final String formattedTableName) {
		final String formattedColumnName = formatColumnName(primaryKeyColumnName);
		final String sql = "DROP SEQUENCE " + getPrimaryKeySequenceName(formattedTableName, formattedColumnName);
		return sql;
	}
}
