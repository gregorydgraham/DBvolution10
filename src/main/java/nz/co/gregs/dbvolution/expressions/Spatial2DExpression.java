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
	 * @return a NumberExpression
	 */
	NumberExpression measurableDimensions();

	/**
	 * Return a rectangular Polygon2D that fully encompasses all point(s) within
	 * this value.
	 *
	 * @return a Polygon2D expression
	 */
	public Polygon2DExpression boundingBox();

	/**
	 * Return a expression that provides the largest X value within the spatial
	 * value.
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression getMaxX();

	/**
	 * Return a expression that provides the largest Y value within the spatial
	 * value.
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression getMaxY();

	/**
	 * Return a expression that provides the smallest X value within the spatial
	 * value.
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression getMinX();

	/**
	 * Return a expression that provides the smallest Y value within the spatial
	 * value.
	 *
	 * @return a NumberExpression
	 */
	public NumberExpression getMinY();

	/**
	 * Return the spatial dimensions of this expression.
	 *
	 * <p>
	 * This represents spatial dimensions in the sense of how many dimensions the
	 * locations of the geometry in defined in. That is to say how many of X, Y,
	 * Z, and W the geometry must be defined in.
	 *
	 * <p>
	 * For instance 2D geometries have 2 spatial dimensions: X and Y. 3D
	 * geometries use X, Y, and Z. Presumably hyper-geometries have X, Y, Z, W,
	 * etc dimensions.
	 *
	 * @return a number expression representing the number of spatial dimensions
	 */
	NumberExpression spatialDimensions();

	/**
	 * Indicates whether this geometry has a magnitude value.
	 *
	 * <p>
	 * Magnitude is a single value that has been measured at this point. For
	 * instance density, depth, color, or temperature. The particular meaning of
	 * magnitude is not defined and it can be anything with a numerical value.
	 *
	 * <p>
	 * Magnitude isn't actually a very good way to store this information but it
	 * is used sometimes.
	 *
	 * @return TRUE if this spatial type supports magnitude values.
	 */
	BooleanExpression hasMagnitude();

	/**
	 * Returns the numerical magnitude value, if any, for this expression.
	 *
	 * <p>
	 * Magnitude is a single value that has been measured at this point. For
	 * instance density, depth, color, or temperature. The particular meaning of
	 * magnitude is not defined and it can be anything with a numerical value.
	 *
	 * <p>
	 * Magnitude isn't actually a very good way to store this information but it
	 * is used sometimes.
	 *
	 * <p>
	 * This value will be null if the actual value is null OR if the spatial type
	 * does not support magnitudes.
	 *
	 * @return a NumberExpression for the magnitude value of this spatial value.
	 */
	NumberExpression magnitude();

	/**
	 * Transform the value into the Well Known Text (WKT) format.
	 *
	 * @return an expression that converts the geometry into the WKT format.
	 */
	StringExpression toWKTFormat();

}
