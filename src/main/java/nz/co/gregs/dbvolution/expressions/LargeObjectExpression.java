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

import nz.co.gregs.dbvolution.results.LargeObjectResult;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeBinary;

/**
 * LargeObjectExpression exposes database expressions for manipulating BLOBs,
 * CLOBs, and JavaObjects.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class LargeObjectExpression extends AnyExpression<byte[], LargeObjectResult, DBLargeBinary> implements LargeObjectResult, ExpressionColumn<DBLargeBinary> {

	private final static long serialVersionUID = 1l;

	private final LargeObjectResult blobResult;
	private final boolean nullProtectionRequired;

	/**
	 * Default Constructor.
	 */
	protected LargeObjectExpression() {
		blobResult = new DBLargeBinary();
		nullProtectionRequired = false;
	}

	/**
	 * Wraps the LargeObjectResult with a LargeObjectExpression to allow further
	 * processing.
	 *
	 * @param originalBlob	originalBlob
	 */
	public LargeObjectExpression(LargeObjectResult originalBlob) {
		blobResult = originalBlob;
		nullProtectionRequired = originalBlob == null || originalBlob.getIncludesNull();
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return blobResult.toSQLString(db);
	}

	@Override
	public LargeObjectExpression copy() {
		return new LargeObjectExpression(blobResult==null?null:blobResult.copy());
	}

	@Override
	public DBLargeBinary getQueryableDatatypeForExpressionValue() {
		return new DBLargeBinary();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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

	@Override
	public DBLargeBinary asExpressionColumn() {
		return new DBLargeBinary(this);
	}

	@Override
	public String createSQLForFromClause(DBDatabase database) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public boolean isComplexExpression() {
		return false;
	}

	@Override
	public LargeObjectResult expression(byte[] value) {
		return expression(new DBLargeBinary(value));
	}

	@Override
	public LargeObjectResult expression(LargeObjectResult value) {
		return new LargeObjectExpression(value);
	}

	@Override
	public LargeObjectResult expression(DBLargeBinary value) {
		return new LargeObjectExpression(value);
	}

}
