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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.query.QueryDetails;

/**
 *
 * @author gregorygraham
 */
public class ExistsExpression extends BooleanExpression {

	QueryDetails outerQuery = new QueryDetails();
	QueryDetails innerQuery = new QueryDetails();

	public ExistsExpression(List<DBRow> outerTables, List<DBRow> innerTables) {
		for (DBRow outerTable : outerTables) {
			final DBRow newOuter = DBRow.copyDBRow(outerTable);
			newOuter.setReturnFieldsToNone();
			this.outerQuery.getRequiredQueryTables().add(newOuter);
			this.outerQuery.getAllQueryTables().add(newOuter);
		}
		for (DBRow innerTable : innerTables) {
			final DBRow newInner = DBRow.copyDBRow(innerTable);
			newInner.setReturnFields(newInner.getPrimaryKey());
			this.innerQuery.getRequiredQueryTables().add(newInner);
		}
	}

	public ExistsExpression(DBRow outerTable, List<DBRow> innerTables) {
		final DBRow newOuter = DBRow.copyDBRow(outerTable);
		newOuter.setReturnFieldsToNone();
		this.outerQuery.getRequiredQueryTables().add(newOuter);
		this.outerQuery.getAllQueryTables().add(newOuter);
		for (DBRow innerTable : innerTables) {
			final DBRow newInner = DBRow.copyDBRow(innerTable);
			newInner.setReturnFields(newInner.getPrimaryKey());
			this.innerQuery.getRequiredQueryTables().add(newInner);
			this.innerQuery.getAllQueryTables().add(newInner);
		}
	}

	public ExistsExpression(DBQuery outerQuery, DBQuery innerQuery) {
		for (DBRow outerTable : outerQuery.getAllTables()) {
			final DBRow newOuter = DBRow.copyDBRow(outerTable);
			newOuter.setReturnFieldsToNone();
			this.outerQuery.getRequiredQueryTables().add(newOuter);
			this.outerQuery.getAllQueryTables().add(newOuter);
		}
		for (DBRow innerTable : innerQuery.getRequiredTables()) {
			final DBRow newInner = DBRow.copyDBRow(innerTable);
			newInner.setReturnFields(newInner.getPrimaryKey());
			this.innerQuery.getRequiredQueryTables().add(newInner);
			this.innerQuery.getAllQueryTables().add(newInner);
		}
		for (DBRow innerTable : innerQuery.getOptionalTables()) {
			final DBRow newInner = DBRow.copyDBRow(innerTable);
			newInner.setReturnFields(newInner.getPrimaryKey());
			this.innerQuery.getOptionalQueryTables().add(newInner);
			this.innerQuery.getAllQueryTables().add(newInner);
		}
	}

	public ExistsExpression(DBRow outerTable, DBRow innerTable) {
		final DBRow newOuter = DBRow.copyDBRow(outerTable);
		newOuter.setReturnFieldsToNone();
		this.outerQuery.getRequiredQueryTables().add(newOuter);
		this.outerQuery.getAllQueryTables().add(newOuter);
		final DBRow newInner = DBRow.copyDBRow(innerTable);
		newInner.setReturnFields(newInner.getPrimaryKey());
		this.innerQuery.getRequiredQueryTables().add(newInner);
			this.innerQuery.getAllQueryTables().add(newInner);
	}

	@Override
	public String toSQLString(DBDatabase db) {
		final List<DBRow> allQueryTables = outerQuery.getAllQueryTables();
		DBQuery dbQuery = db
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
