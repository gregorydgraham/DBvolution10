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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.query.QueryDetails;
import nz.co.gregs.dbvolution.query.QueryOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provides support for the abstract concept of validating migration rows from
 * one or more tables to another table.
 *
 *
 * @author Gregory Graham
 * @param <R>
 */
public class DBValidation<R extends DBRow> {

	private static final Log LOG = LogFactory.getLog(DBValidation.class);

	private static String PROCESSED_COLUMN = "Processed Column";

	private final DBMigration<R> sourceMigration;
	private final DBRow[] extraExamples;

	/**
	 * Creates a DBValidate action for the table.
	 *
	 * @param migration the mapping to migrate
	 * @param source
	 * @param examples
	 */
	public DBValidation(DBMigration<R> migration, DBRow source, DBRow... examples) {
		sourceMigration = migration;
		extraExamples = examples;
	}

	public Results validate(DBDatabase database) throws SQLException {

		QueryDetails details = sourceMigration.getQueryDetails();
		QueryOptions options = details.getOptions();
		List<BooleanExpression> conditions = details.getAllConditions();
		List<DBRow> allQueryTables = details.getAllQueryTables();

		DBQuery dbQuery = database.getDBQuery()
				.setBlankQueryAllowed(options.isBlankQueryAllowed())
				.setCartesianJoinsAllowed(options.isCartesianJoinAllowed());
		for (DBRow tab : allQueryTables) {
			dbQuery.addOptional(DBRow.getDBRow(tab.getClass()));
		}
		final BooleanExpression criteria = BooleanExpression.allOf(conditions.toArray(new BooleanExpression[]{}));

		dbQuery.addExpressionColumn(PROCESSED_COLUMN, criteria);
		//dbQuery.addToSortOrder(criteria);

		String sqlForQuery = dbQuery.getSQLForQuery();
		System.err.println("" + sqlForQuery);

		final List<DBQueryRow> allRows = dbQuery.getAllRows();
		Results results = new Results(allRows);

		return results;
	}

	public static class Results extends ArrayList<Result> {

		private List<DBQueryRow> rows;

		private Results() {
			super();
		}

		private Results(List<DBQueryRow> rows) {
			this.rows = rows;
			for (DBQueryRow row : rows) {
				this.add(new Result(row));
			}
		}
	}

	public static class Result {

		private DBQueryRow row = null;
		public Boolean willBeProcessed = false;

		private Result() {
		}

		private Result(DBQueryRow row) {
			this.row = row;
			final QueryableDatatype<?> expressionColumnValue = row.getExpressionColumnValue(DBValidation.PROCESSED_COLUMN);
			if (expressionColumnValue != null && expressionColumnValue instanceof DBBoolean) {
				this.willBeProcessed = ((DBBoolean) expressionColumnValue).booleanValue();
			}
		}

		<A extends DBRow> A getRow(A exemplar) {
			return row.get(exemplar);
		}
	}
}
