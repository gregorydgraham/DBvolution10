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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import nz.co.gregs.dbvolution.columns.StringColumn;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
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
	private final DBRow mapper;

	/**
	 * Creates a DBValidate action for the table.
	 *
	 * @param migration the mapping to migrate
	 * @param mapper
	 * @param examples
	 */
	public DBValidation(DBMigration<R> migration, DBRow mapper, DBRow... examples) {
		sourceMigration = migration;
		this.mapper = mapper;
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

		addAllTablesToValidationQuery(allQueryTables, dbQuery);
		addProcessingColumnToValidationQuery(conditions, dbQuery);

		addDataCheckingColumnsToValidationQuery(dbQuery);

		String sqlForQuery = dbQuery.getSQLForQuery();
		System.err.println("" + sqlForQuery);

		final List<DBQueryRow> allRows = dbQuery.getAllRows();
		Results results = new Results(mapper, allRows);

		return results;
	}

	private void addDataCheckingColumnsToValidationQuery(DBQuery dbQuery) {
		List<PropertyWrapper> properties = mapper.getColumnPropertyWrappers();
		for (PropertyWrapper prop : properties) {
			QueryableDatatype<?> qdt = prop.getDefinition().getQueryableDatatype(mapper);
			if (qdt instanceof DBString) {
				StringColumn column = mapper.column((DBString) qdt);
				dbQuery.addExpressionColumn(prop,
						column.isNullOrEmpty().ifThenElse(StringExpression.value("NO DATA"), StringExpression.value("success")));
			}
		}
	}

	private void addProcessingColumnToValidationQuery(List<BooleanExpression> conditions, DBQuery dbQuery) {
		final BooleanExpression criteria = BooleanExpression.allOf(conditions.toArray(new BooleanExpression[]{}));

		dbQuery.addExpressionColumn(PROCESSED_COLUMN, criteria);
		//dbQuery.addToSortOrder(criteria);
	}

	private void addAllTablesToValidationQuery(List<DBRow> allQueryTables, DBQuery dbQuery) throws UnableToInstantiateDBRowSubclassException {
		for (DBRow tab : allQueryTables) {
			dbQuery.addOptional(DBRow.getDBRow(tab.getClass()));
		}
	}

	public static class Results extends ArrayList<Result> {

		protected final static long serialVersionUID = 1L;
		
		private List<DBQueryRow> rows;
		private DBRow mapper;

		private Results() {
			super();
		}

		private Results(DBRow mapper, List<DBQueryRow> rows) {
			this.mapper = mapper;
			this.rows = rows;
			for (DBQueryRow row : rows) {
				this.add(new Result(mapper, row));
			}
		}
	}

	public static class Result {

		public Boolean willBeProcessed = false;

		private DBQueryRow row = null;
		private DBRow mapper = null;
		private final Map<String, String> map = new HashMap<>();

		private Result() {
		}

		private Result(DBRow mapper, DBQueryRow row) {
			this.mapper = mapper;
			this.row = row;
			final QueryableDatatype<?> expressionColumnValue = row.getExpressionColumnValue(DBValidation.PROCESSED_COLUMN);
			if (expressionColumnValue != null && expressionColumnValue instanceof DBBoolean) {
				this.willBeProcessed = ((DBBoolean) expressionColumnValue).booleanValue();
			}

			List<PropertyWrapper> properties = mapper.getColumnPropertyWrappers();
			for (PropertyWrapper prop : properties) {
				QueryableDatatype<?> propColumnValue = row.getExpressionColumnValue(prop);
				this.map.put(prop.javaName(), propColumnValue.stringValue());
			}
		}

		<A extends DBRow> A getRow(A exemplar) {
			return row.get(exemplar);
		}
		
		public Map<String, String> getMap(){
			return new HashMap<>(this.map);
		}
	}
}