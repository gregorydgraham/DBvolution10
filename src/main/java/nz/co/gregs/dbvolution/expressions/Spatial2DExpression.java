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
package nz.co.gregs.dbvolution.expressions;

import nz.co.gregs.dbvolution.results.Spatial2DResult;

/**
 * Designates the expression as a Spatial2D expression.
 *
 * @author gregorygraham
 */
public interface Spatial2DExpression extends Spatial2DResult {

	/**
	 * Return the measurableDimensions of this expression.
	 *
	 * <p>
	 * This represents measurableDimensions in the sense of how many dimensions
	 * they can be measured in.
	 *
	 * <p>
	 * For instance points are zero dimensional in this sense, while lines have a
	 * single measurableDimensions, and polygons have 2.
	 *
	 * <p>
	 * Since this is a 2D expression, the maximum value of measurableDimensions()
	 * is 2.
	 *
	 * <p>
	 * All Spatial2D values are still defined in 2D space and require an X and a Y
	 * value.
	 *
	 * @return
	 */
	@Override
	NumberExpression measurableDimensions();

	/**
	 * Return a rectangular Polygon2D that fully encompasses all point(s) within
	 * this value.
	 *
	 * @return a Polygon2D expression
	 */
	public Polygon2DExpression boundingBox();

}
