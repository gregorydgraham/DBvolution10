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
package nz.co.gregs.dbvolution.expressions;

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBByteArray;

/**
 * LargeObjectExpression exposes database expressions for manipulating BLOBs,
 * CLOBs, and JavaObjects.
 *
 * @author gregorygraham
 */
public class LargeObjectExpression implements LargeObjectResult {

	private final LargeObjectResult blobResult;
	private boolean nullProtectionRequired = false;

	/**
	 * Default Constructor.
	 */
	protected LargeObjectExpression() {
		blobResult = new DBByteArray();
	}

	/**
	 * Wraps the LargeObjectResult with a LargeObjectExpression to allow further
	 * processing.
	 *
	 * @param originalBlob	 originalBlob	
	 */
	public LargeObjectExpression(LargeObjectResult originalBlob) {
		blobResult = originalBlob;
		if (originalBlob == null || originalBlob.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	@Override
	public String toSQLString(DBDatabase db) {
		return blobResult.toSQLString(db);
	}

	@Override
	public LargeObjectExpression copy() {
		return new LargeObjectExpression(blobResult.copy());
	}

	@Override
	public DBByteArray getQueryableDatatypeForExpressionValue() {
		return new DBByteArray();
	}

	@Override
	public boolean isAggregator() {
		return blobResult.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (blobResult != null) {
			hashSet.addAll(blobResult.getTablesInvolved());
		}
		return hashSet;
	}

	/**
	 * Tests the LargeObjectExpression to see if it is not NULL in the database.
	 *
	 * @return a BooleanExpression to use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression isNotNull() {
		return BooleanExpression.isNotNull(this);
	}

	/**
	 * Tests the LargeObjectExpression to see if it is NULL in the database.
	 *
	 * @return a BooleanExpression to use in {@link DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
	 * }
	 */
	public BooleanExpression isNull() {
		return BooleanExpression.isNull(this);
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

		@Override
		public boolean isPurelyFunctional() {
			if (blobResult == null) {
				return true; // this should never occur, just sayin'
			} else {
				return blobResult.isPurelyFunctional();
			}
		}

}
