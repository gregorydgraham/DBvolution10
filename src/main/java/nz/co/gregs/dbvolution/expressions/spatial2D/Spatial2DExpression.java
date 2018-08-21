/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.expressions.spatial2D;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.EqualExpression;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.Spatial2DComparable;
import nz.co.gregs.dbvolution.results.Spatial2DResult;

/**
 * Designates the expression as a Spatial2D expression.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 * @param <B>
 * @param <R>
 * @param <D>
 */
public abstract class Spatial2DExpression< B, R extends Spatial2DResult<B>, D extends QueryableDatatype<B>> extends EqualExpression<B, R, D> implements Spatial2DComparable<B, R> {

	private final static long serialVersionUID = 1l;

	/**
	 *
	 * @param only
	 */
	protected Spatial2DExpression(R only) {
		super(only);
	}	
	protected Spatial2DExpression() {
		super();
	}
	/**
	 *
	 * @param only
	 */
	protected Spatial2DExpression(AnyResult<?> only) {
		super(only);
	}
	


}
