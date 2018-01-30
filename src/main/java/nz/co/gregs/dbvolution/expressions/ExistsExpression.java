/*
 * Copyright 2014 gregorygraham.
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

import nz.co.gregs.dbvolution.databases.DBDatabase;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.internal.query.QueryDetails;

/**
 * Creates an expression that implements the EXISTS operation.
 *
 * <p>
 * EXISTS is an expensive operation that should be avoided, however it is
 * sometimes useful.
 *
 * <p>
 * EXISTS creates a subquery that finds rows matching the examples provided and
 * returns them to the actual query by connecting the tables in the subquery to
 * the tables in the primary query.
 *
 * <p>
 * {@code query.addCondition(new ExistsExpression(tableA, tableB));}
 *
 * <p>
 * This is most useful when negated as it allows you to find rows based on the
 * non-existence of a relationship. For instance all Dealerships that have not
 * sold a car this period:
 *
 * <p>
 * {@code
 * DBQuery query = database.getDBQuery(new Dealership());
 * CarSale carSales = new CarSales();
 * carSales.salesPeriod.permittedValues("Current");
 * ExistsExpression exists = new ExistsExpression(new Dealership(), carSales);
 * query.addCondition(exists.not());
 * }
 *
 * <p>
 * Without EXISTS this query would need 2 queries: find all the dealerships that
 * have had sales; find all dealerships not in the previous list.
 *
 * <p>
 * However, EXISTS is an expensive operation and you may get better results
 * using the 2 queries.
 *
 * <p>
 * Another alternative is to add CarSales as an optional table and ignore rows
 * with a non-null CarSales.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class ExistsExpression extends BooleanExpression {

	private static final long serialVersionUID = 1l;

	private final QueryDetails outerQuery = new QueryDetails();
	private final QueryDetails innerQuery = new QueryDetails();
	private final DBDatabase database;

	/**
	 * Create an ExistsExpression that connects to the original query via the
	 * tables of the first query supplied, and uses the examples in the second.
	 *
	 * @param outerQuery a query that will also be linked to the exterior query.
	 * @param innerQuery a query which will not be linked to the exterior query.
	 */
	public ExistsExpression(DBQuery outerQuery, DBQuery innerQuery) {
		this.database = outerQuery.getDatabase();
		for (DBRow outerTable : outerQuery.getAllTables()) {
			final DBRow newOuter = DBRow.copyDBRow(outerTable);
			newOuter.setReturnFieldsToNone();
			this.outerQuery.getRequiredQueryTables().add(newOuter);
			this.outerQuery.getAllQueryTables().add(newOuter);
		}
		for (DBRow innerTable : innerQuery.getRequiredTables()) {
			final DBRow newInner = DBRow.copyDBRow(innerTable);
			newInner.setReturnFields((Object[]) newInner.getPrimaryKeysAsArray());
			this.innerQuery.getRequiredQueryTables().add(newInner);
			this.innerQuery.getAllQueryTables().add(newInner);
		}
		for (DBRow innerTable : innerQuery.getOptionalTables()) {
			final DBRow newInner = DBRow.copyDBRow(innerTable);
			newInner.setReturnFields((Object[]) newInner.getPrimaryKeysAsArray());
			this.innerQuery.getOptionalQueryTables().add(newInner);
			this.innerQuery.getAllQueryTables().add(newInner);
		}
	}

	/**
	 * Produces the snippet provided by this class.
	 *
	 * <p>
	 * This is only used internally.
	 *
	 * <p>
	 * If you are extending DBvolution and adding a new function this is the place
	 * to format the information for use in SQL. A DBDefinition instance is
	 * provided to supply context and so your SQL can used on multiple database
	 * engines.
	 *
	 * @param defn the target database
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the DBValue formatted as a SQL snippet
	 */
	@Override
	public String toSQLString(DBDefinition defn) {
		final List<DBRow> allQueryTables = outerQuery.getAllQueryTables();
		DBQuery dbQuery
				= database
						.getDBQuery(innerQuery.getRequiredQueryTables())
						.addOptional(innerQuery.getOptionalQueryTables())
						.addAssumedTables(allQueryTables);
		String sql = dbQuery.getSQLForQuery().replaceAll(";", "");
		return " EXISTS (" + sql + ")";
	}

	@Override
	@SuppressWarnings(value = "unchecked")
	public ExistsExpression copy() {
		ExistsExpression clone;
		try {
			clone = (ExistsExpression) this.clone();
		} catch (CloneNotSupportedException ex) {
			throw new RuntimeException(ex);
		}
		return clone;
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		final HashSet<DBRow> hashSet = new HashSet<DBRow>();
		hashSet.addAll(outerQuery.getAllQueryTables());
		return hashSet;
	}
}
