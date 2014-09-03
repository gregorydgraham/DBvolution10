/*
 * Copyright 2014 Gregory Graham.
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

package nz.co.gregs.dbvolution.datatypes;

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;

/**
 * Indicates that the 'type' is implied by other details.
 */
public class ImplicitQueryableDatatype extends QueryableDatatype {
	private static final long serialVersionUID = 1L;

	private ImplicitQueryableDatatype() {
	}

	@Override
	public String getSQLDatatype() {
		return null;
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		return null;
	}

	@Override
	void setValue(Object newLiteralValue) {
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
