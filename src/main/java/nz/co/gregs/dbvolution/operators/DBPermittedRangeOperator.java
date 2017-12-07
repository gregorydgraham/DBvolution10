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
package nz.co.gregs.dbvolution.operators;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Implements a BETWEEN operator that includes the beginning of the range but
 * excludes the end.
 *
 * <p>
 * Given a set of 1,2,3,4,5, using DBPermittedRangeExclusiveOperator(1,5) will
 * produce an operation such that 1, 2, 3, and 4 will be returned, but not 5.
 *
 * <p>
 * Unbounded ranges are created by passing null as the lower- or upper-bound.
 * For example, DBPermittedRangeExclusiveOperator(1,null) will produce an
 * operation such that every number larger than 1 will be returned.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <T>
 */
public class DBPermittedRangeOperator<T> extends DBMetaOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Implements a BETWEEN operator that includes the beginning of the range but
	 * excludes the end.
	 *
	 * <p>
	 * Use a null value to create open or unbounded ranges.
	 *
	 * @param lowerBound the smallest value of the desired range
	 * @param upperBound the smallest value larger than the desired range
	 */
	public DBPermittedRangeOperator(T lowerBound, T upperBound) {
		if (lowerBound != null && upperBound != null) {
			operator = new DBBetweenInclusiveExclusiveOperator(
					QueryableDatatype.getQueryableDatatypeForObject(lowerBound),
					QueryableDatatype.getQueryableDatatypeForObject(upperBound));
		} else if (lowerBound == null && upperBound != null) {
			QueryableDatatype<?> qdt = QueryableDatatype.getQueryableDatatypeForObject(upperBound);
			operator = new DBLessThanOperator(qdt);
		} else if (lowerBound != null && upperBound == null) {
			final QueryableDatatype<?> qdt = QueryableDatatype.getQueryableDatatypeForObject(lowerBound);
			operator = new DBGreaterThanOrEqualsOperator(qdt);
		}
	}

}
