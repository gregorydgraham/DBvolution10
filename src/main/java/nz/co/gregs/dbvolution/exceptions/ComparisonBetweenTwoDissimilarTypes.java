/*
 * Copyright 2015 gregory.graham.
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
package nz.co.gregs.dbvolution.exceptions;

import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 * Thrown when the two data types used in a comparison does not support that
 * comparison.
 *
 * <p>
 * Theoretically no-one should ever see this exception but mistakes do happen.
 *
 * <p>
 * If it occurs check that you're not accidentally comparing LargeObject columns
 * (JavaObject, CLOB, BLOB, etc) as they support very few comparisons.
 *
 * <p>
 * Alternatively inform the developers and they will fix it.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class ComparisonBetweenTwoDissimilarTypes extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Thrown when the two data types used in a comparison does not support that
	 * comparison.
	 *
	 * <p>
	 * Theoretically no-one should ever see this exception but mistakes do happen.
	 *
	 * <p>
	 * If it occurs check that you're not accidentally comparing LargeObject
	 * columns (JavaObject, CLOB, BLOB, etc) as they support very few comparisons.
	 *
	 * <p>
	 * Alternatively inform the developers and they will fix it.
	 *
	 * @param db
	 * @param genericExpression
	 * @param firstValue
	 */
	public ComparisonBetweenTwoDissimilarTypes(DBDefinition db, DBExpression genericExpression, DBExpression firstValue) {
		super("Attempt To Compared Two Dissimilar Types: "
				+ genericExpression.toSQLString(db)
				+ " is a " + genericExpression.getClass().getSimpleName()
				+ " and cannot be compared to " + firstValue.toSQLString(db)
				+ " which is a " + firstValue.getClass().getSimpleName());
	}

}
