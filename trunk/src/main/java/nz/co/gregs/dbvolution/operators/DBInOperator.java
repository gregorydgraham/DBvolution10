/*
 * Copyright 2013 gregorygraham.
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
package nz.co.gregs.dbvolution.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer.DBSafeInternalQDTAdaptor;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 *
 * @author gregorygraham
 */
public class DBInOperator extends DBOperator {

	public static final long serialVersionUID = 1L;
	protected final List<QueryableDatatype> listOfPossibleValues = new ArrayList<QueryableDatatype>();

	public DBInOperator(List<QueryableDatatype> listOfPossibleValues) {
		super();
		for (QueryableDatatype qdt : listOfPossibleValues) {
			this.listOfPossibleValues.add(qdt == null ? null : qdt.copy());
		}
	}

	public DBInOperator(Set<QueryableDatatype> listOfPossibleValues) {
		super();
		for (QueryableDatatype qdt : listOfPossibleValues) {
			this.listOfPossibleValues.add(qdt == null ? null : qdt.copy());
		}
	}

	public DBInOperator(QueryableDatatype[] listOfPossibleValues) {
		super();
		for (QueryableDatatype qdt : listOfPossibleValues) {
			this.listOfPossibleValues.add(qdt == null ? null : qdt.copy());
		}
	}

	public DBInOperator() {
		super();
	}

	@Override
	public String generateWhereLine(DBDatabase db, String columnName) {
		DBDefinition defn = db.getDefinition();
		StringBuilder whereClause = new StringBuilder();
		if (listOfPossibleValues.isEmpty()) {
			// prevent any rows from returning as an empty list means no rows can match
			whereClause.append(defn.getFalseOperation());
		} else {
			whereClause.append(columnName);
			whereClause.append(invertOperator ? getInverse() : getOperator());
			String sep = "";
			for (QueryableDatatype qdt : listOfPossibleValues) {
				whereClause.append(sep).append(" ").append(qdt.toSQLString(db)).append(" ");
				sep = ",";
			}
			if (this.includeNulls && defn.supportsDifferenceBetweenNullAndEmptyString()) {
				whereClause.append(sep).append(" ").append(defn.getEmptyString()).append(" ");
			}
			whereClause.append(")");
		}
		if (this.includeNulls) {
			DBIsNullOperator dbIsNullOperator = new DBIsNullOperator();
			dbIsNullOperator.invertOperator(this.invertOperator);
			return "(" + dbIsNullOperator.generateWhereLine(db, columnName) + (this.invertOperator ? defn.beginAndLine() : defn.beginOrLine()) + whereClause.toString() + ")";
		} else {
			return whereClause.toString();
		}
	}

	protected String getOperator() {
		return " in (";
	}

	protected String getInverse() {
		return " not in (";
	}

//	@Override
//	public String generateRelationship(DBDatabase database, String columnName, String otherColumnName) {
//		return columnName + (invertOperator ? getInverse() : getOperator()) + otherColumnName + " ) ";
//	}

	@Override
	public DBOperator getInverseOperator() {
		return this;
	}

	@Override
	public boolean equals(DBOperator other) {

		if (super.equals(other) == false) {
			return false;
		} else {
			if (other instanceof DBInOperator) {
				DBInOperator otherIn = (DBInOperator) other;
				if (listOfPossibleValues.size() != otherIn.listOfPossibleValues.size()) {
					return false;
				} else {
					for (QueryableDatatype qdt : listOfPossibleValues) {
						if (!otherIn.listOfPossibleValues.contains(qdt)) {
							return false;
						}
					}
				}
			} else {
				return false;
			}
			return true;
		}
	}

	@Override
	public DBInOperator copyAndAdapt(DBSafeInternalQDTAdaptor typeAdaptor) {
		List<QueryableDatatype> list = new ArrayList<QueryableDatatype>();
		for (QueryableDatatype item : listOfPossibleValues) {
			list.add((QueryableDatatype) typeAdaptor.convert(item));
		}
		DBInOperator op = new DBInOperator(list);
		op.invertOperator = this.invertOperator;
		op.includeNulls = this.includeNulls;
		return op;
	}
}
