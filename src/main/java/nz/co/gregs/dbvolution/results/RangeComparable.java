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
package nz.co.gregs.dbvolution.results;

import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 * Indicates that the class can be compared to other instances of this class as
 * if the instances were part of a range.
 *
 * <p>
 * Methods appropriate to a range include Greater Than, Less Than, and Equals.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <B> base class
 * @param <A> the class that can be compared
 */
public interface RangeComparable<B, A extends DBExpression> extends RangeResult<B>, InComparable<B, A> {

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * LESSTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isLessThan(A anotherInstance);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * LESSTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isLessThan(B anotherInstance);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * GREATERTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThan(A anotherInstance);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * LESSTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isLessThanOrEqual(A anotherInstance);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * GREATERTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThanOrEqual(A anotherInstance);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * LESSTHAN operation.
	 *
	 * <p>
	 * However when the 2 values are equal the results of the fallback expression
	 * is returned instead. This helps apply the LESSTHAN operator across multiple
	 * columns.
	 *
	 * @param anotherInstance the instance to compare to
	 * @param fallBackWhenEqual expression to use when the values are equal
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isLessThan(A anotherInstance, BooleanExpression fallBackWhenEqual);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * GREATERTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * @param fallBackWhenEqual expression to use when the values are equal
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThan(A anotherInstance, BooleanExpression fallBackWhenEqual);

	public BooleanExpression isBetween(A anotherInstance, A largerExpression);

	public BooleanExpression isBetweenInclusive(A anotherInstance, A largerExpression);

	public BooleanExpression isBetweenExclusive(A anotherInstance, A largerExpression);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * GREATERTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThan(B anotherInstance);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * LESSTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isLessThanOrEqual(B anotherInstance);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * GREATERTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThanOrEqual(B anotherInstance);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * LESSTHAN operation.
	 *
	 * <p>
	 * However when the 2 values are equal the results of the fallback expression
	 * is returned instead. This helps apply the LESSTHAN operator across multiple
	 * columns.
	 *
	 * @param anotherInstance the instance to compare to
	 * @param fallBackWhenEqual expression to use when the values are equal
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isLessThan(B anotherInstance, BooleanExpression fallBackWhenEqual);

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * GREATERTHAN operation.
	 *
	 * @param anotherInstance the instance to compare to
	 * @param fallBackWhenEqual expression to use when the values are equal
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a BooleanExpression
	 */
	public BooleanExpression isGreaterThan(B anotherInstance, BooleanExpression fallBackWhenEqual);

	public BooleanExpression isBetween(B anotherInstance, B largerExpression);

	public BooleanExpression isBetweenInclusive(B anotherInstance, B largerExpression);

	public BooleanExpression isBetweenExclusive(B anotherInstance, B largerExpression);

	public BooleanExpression isBetween(B anotherInstance, A largerExpression);

	public BooleanExpression isBetweenInclusive(B anotherInstance, A largerExpression);

	public BooleanExpression isBetweenExclusive(B anotherInstance, A largerExpression);

	public BooleanExpression isBetween(A anotherInstance, B largerExpression);

	public BooleanExpression isBetweenInclusive(A anotherInstance, B largerExpression);

	public BooleanExpression isBetweenExclusive(A anotherInstance, B largerExpression);

}
