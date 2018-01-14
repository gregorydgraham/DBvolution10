/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.results;

import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 * Indicates that the class can be compared to several other instances of this
 * class as if the instances were equivalent.
 *
 * <p>
 * EqualsComparable expressions must have an equivalent to the IN operation.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <B> a base type like String or Long
 * @param <A> the class that can be compared using the "IN" operator
 *
 */
public interface InComparable<B, A extends DBExpression> extends InResult<B>, EqualComparable<B, A> {

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 values using the IN
	 * operation.
	 *
	 * @param otherInstances the values which are to be considered
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	@SuppressWarnings({"unchecked", "varargs"})
	public BooleanExpression isIn(A... otherInstances);
}
