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
package nz.co.gregs.dbvolution.expressions;

import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.LineSegment3DResult;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial3D.DBLineSegment3D;
import nz.co.gregs.dbvolution.datatypes.spatial3D.LineSegmentZ;

/**
 * Represents expressions that produce a geometry consisting of 2 points and
 * representing a line from the first point to the second.
 *
 * @author gregory.graham
 */
public class LineSegment3DExpression implements LineSegment3DResult, EqualComparable<LineSegment3DResult>, Spatial3DExpression, ExpressionColumn<DBLineSegment3D> {

	private LineSegment3DResult innerLineString;
	private boolean nullProtectionRequired;

	/**
	 * Default constructor.
	 *
	 */
	protected LineSegment3DExpression() {
	}

	/**
	 * Create a LineSegment3D expression encapsulating the value supplied.
	 *
	 * @param value
	 */
	public LineSegment3DExpression(LineSegment3DResult value) {
		initInnerLine(value, value, value);
	}

	private void initInnerLine(Object original1, Object original2, LineSegment3DResult value) {
		innerLineString = value;
		if (original1 == null || original2 == null || innerLineString.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a LineSegment3D expression encapsulating the value supplied.
	 *
	 * @param line
	 */
	public LineSegment3DExpression(LineSegmentZ line) {
		innerLineString = new DBLineSegment3D(line);
		initInnerLine(line, line, innerLineString);
	}

	/**
	 * Create a LineSegment3D expression encapsulating the values supplied.
	 *
	 * @param point1x
	 * @param point1y
	 * @param point2x
	 * @param point2y
	 */
	public LineSegment3DExpression(Double point1x, Double point1y, Double point1z, Double point2x, Double point2y, Double point2z) {
		LineSegmentZ line = null;
		if (point1x != null && point1y != null && point1z != null && point2x != null && point2y != null && point2z != null) {
			line = new LineSegmentZ(point1x, point1y, point1z, point2x, point2y, point2z);
			innerLineString = new DBLineSegment3D(line);
		} else {
			innerLineString = new DBLineSegment3D(line);
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a LineSegment3D expression encapsulating the value supplied.
	 *
	 * @param point1
	 * @param point2
	 *
	 */
	public LineSegment3DExpression(Point point1, Point point2) {
		LineSegmentZ line = null;
		if (point1 != null && point2 != null) {
			line = new LineSegmentZ(point1.getCoordinate(), point2.getCoordinate());
			innerLineString = new DBLineSegment3D(line);
		} else {
			innerLineString = new DBLineSegment3D(line);
		}
		initInnerLine(point1, point2, innerLineString);
	}

	/**
	 * Create a LineSegment3D expression encapsulating the value supplied.
	 *
	 * @param coord1
	 * @param coord2
	 */
	public LineSegment3DExpression(Coordinate coord1, Coordinate coord2) {
		LineSegmentZ line = null;
		if (coord1 != null && coord2 != null) {
			line = new LineSegmentZ(coord1, coord2);
			innerLineString = new DBLineSegment3D(line);
		} else {
			innerLineString = new DBLineSegment3D(line);
		}
		initInnerLine(coord1, coord2, innerLineString);
	}

	/**
	 * Create an expression for the line segment created from the 2 points.
	 *
	 * @param point1 the starting point of this value
	 * @param point2 the end point of this value
	 * @return a LineSegment3D expression
	 */
	public static LineSegment3DExpression value(Point point1, Point point2) {
		return new LineSegment3DExpression(point1, point2);
	}

	/**
	 * Create an expression for the line segment created from the 2 coordinates.
	 *
	 * @param coord1 the starting point of this value
	 * @param coord2 the end point of this value
	 * @return a LineSegment3D expression
	 */
	public static LineSegment3DExpression value(Coordinate coord1, Coordinate coord2) {
		return new LineSegment3DExpression(coord1, coord2);
	}

	/**
	 * Create an expression for the line segment created by combining the 4
	 * numbers into 2 points.
	 *
	 * @param x1 the first X of this value
	 * @param y1 the first Y of this value
	 * @param x2 the last X of this value
	 * @param y2 the last Y of this value
	 * @return a LineSegment3D expression
	 */
	public static LineSegment3DExpression value(Double x1, Double y1, Double z1, Double x2, Double y2, Double z2) {
		return new LineSegment3DExpression(new Coordinate(x1, y1, z1), new Coordinate(x2, y2, z2));
	}

	/**
	 * Create an expression for the line segment created from the 2 points.
	 *
	 * @param line the value of this line expression
	 * @return a LineSegment3D expression
	 */
	public static LineSegment3DExpression value(LineSegmentZ line) {
		return new LineSegment3DExpression(line);
	}

	/**
	 * Create an expression for the line segment created from the 2 points.
	 *
	 * @param line the value of this line expression
	 * @return a LineSegment3D expression
	 */
	public static LineSegment3DExpression value(LineSegment3DResult line) {
		return new LineSegment3DExpression(line);
	}

	@Override
	public DBLineSegment3D getQueryableDatatypeForExpressionValue() {
		return new DBLineSegment3D();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerLineString == null) {
			return db.getDefinition().getNull();
		} else {
			return innerLineString.toSQLString(db);
		}
	}

	@Override
	public LineSegment3DExpression copy() {
		return new LineSegment3DExpression(innerLineString);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof LineSegment3DExpression) {
			LineSegment3DExpression otherExpr = (LineSegment3DExpression) other;
			return this.innerLineString == otherExpr.innerLineString;
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 37 * hash + (this.innerLineString != null ? this.innerLineString.hashCode() : 0);
		hash = 37 * hash + (this.nullProtectionRequired ? 1 : 0);
		return hash;
	}

	@Override
	public boolean isAggregator() {
		return innerLineString == null ? false : innerLineString.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<>();
		if (innerLineString != null) {
			hashSet.addAll(innerLineString.getTablesInvolved());
		}
		return hashSet;
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerLineString == null ? true : innerLineString.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

	@Override
	public StringExpression toWKTFormat() {
		return stringResult();
	}

	/**
	 * Convert this LineSegment3D to a String representation based on the Well
	 * Known Text (WKT) format.
	 *
	 * @return a StringExpression representing this spatial value.
	 */
	@Override
	public StringExpression stringResult() {
		return new StringExpression(new LineSegmentWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment3DAsTextTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().toSQLString(db);
				}
			}
		});
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 instances using the
	 * EQUALS operation.
	 *
	 * @param rightHandSide the value to compare against
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(LineSegmentZ rightHandSide) {
		return is(new DBLineSegment3D(rightHandSide));
	}

	@Override
	public BooleanExpression is(LineSegment3DResult rightHandSide) {
		return new BooleanExpression(new LineSegmentLineSegmentWithBooleanResult(this, new LineSegment3DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment3DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().is(getSecond().stringResult()).toSQLString(db);
				}
			}
		});
	}

	/**
	 * Creates a {@link BooleanExpression} that compares the 2 values using the
	 * NOT EQUALS operation.
	 *
	 * @param rightHandSide the value to compare against
	 * @return a BooleanExpression returning TRUE if the two line segments are
	 * different, otherwise FALSE.
	 */
	public BooleanExpression isNot(LineSegmentZ rightHandSide) {
		return isNot(new DBLineSegment3D(rightHandSide));
	}

	@Override
	public BooleanExpression isNot(LineSegment3DResult rightHandSide) {
		return is(rightHandSide).not();
	}

	@Override
	public NumberExpression measurableDimensions() {
		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment3DDimensionTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(1).toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression spatialDimensions() {
		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment3DSpatialDimensionsTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(2).toSQLString(db);
				}
			}
		});
	}

	@Override
	public BooleanExpression hasMagnitude() {
		return new BooleanExpression(new LineSegmentWithBooleanResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment3DHasMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return BooleanExpression.falseExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression magnitude() {
		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment3DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon3DExpression boundingBox() {
		return new Polygon3DExpression(new LineSegmentWithGeometry3DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment3DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final LineSegment3DExpression first = getFirst();
					final NumberExpression maxX = first.maxX();
					final NumberExpression maxY = first.maxY();
					final NumberExpression minX = first.minX();
					final NumberExpression minY = first.minY();
					final NumberExpression maxZ = first.maxZ();
					final NumberExpression minZ = first.minZ();
					return Polygon3DExpression.value(
							Point3DExpression.value(minX, minY, minZ),
							Point3DExpression.value(maxX, minY, minZ),
							Point3DExpression.value(maxX, maxY, minZ),
							Point3DExpression.value(maxX, maxY, maxZ),
							Point3DExpression.value(minX, maxY, maxZ),
							Point3DExpression.value(minX, minY, maxZ),
							Point3DExpression.value(minX, minY, minZ))
							.toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression maxX() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment3DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression minX() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment3DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression maxY() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment3DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression minY() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment3DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression maxZ() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment3DGetMaxZTransform(getFirst().toSQLString(db));
			}
		});
	}

	@Override
	public NumberExpression minZ() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment3DGetMinZTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Tests whether this line segment and the line segment represented by the
	 * points ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.Point, com.vividsolutions.jts.geom.Point)
	 * } to find the intersection point.
	 *
	 * @param point1 the first point in the line segment to compare against
	 * @param point2 the last point in the line segment to compare against
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Point point1, Point point2) {
		return this.intersects(new LineSegment3DExpression(point1, point2));
	}

	/**
	 * Tests whether this line segment and the line segment represented by the
	 * coordinates ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.Coordinate, com.vividsolutions.jts.geom.Coordinate)
	 * } to find the intersection point.
	 *
	 * @param coord1 the first point in the line segment to compare against
	 * @param coord2 the last point in the line segment to compare against
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Coordinate coord1, Coordinate coord2) {
		return this.intersects(new LineSegment3DExpression(coord1, coord2));
	}

	/**
	 * Tests whether this line segment and the other line segment ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.LineSegment) } to
	 * find the intersection point.
	 *
	 * @param linesegment the value to compare against
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineSegmentZ linesegment) {
		return this.intersects(new LineSegment3DExpression(linesegment));
	}

	/**
	 * Tests whether this line segment and the other line segment ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(nz.co.gregs.dbvolution.results.LineSegment3DResult)
	 * } to find the intersection point.
	 *
	 * @param crossingLine the value to compare against
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineSegment3DResult crossingLine) {
		return new BooleanExpression(new LineSegmentLineSegmentWithBooleanResult(this, new LineSegment3DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment3DIntersectsLineSegment3DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Returns an expression providing the point of intersection between this line
	 * segment and the line segment formed from the two points provided.
	 *
	 * @param point1 the first point of the line segment to compare against
	 * @param point2 the last point of the line segment to compare against
	 * @return a Point3DExpression
	 */
	public Point3DExpression intersectionWith(Point point1, Point point2) {
		return this.intersectionWith(new LineSegment3DExpression(point1, point2));
	}

	/**
	 * Returns an expression providing the point of intersection between this line
	 * segment and the line segment formed from the four ordinates provided.
	 *
	 * @param point1x the first X of the line segment to compare against
	 * @param point1y the first Y of the line segment to compare against
	 * @param point2x the last X of the line segment to compare against
	 * @param point2y the last Y of the line segment to compare against
	 * @return a Point3DExpression
	 */
	public Point3DExpression intersectionWith(Double point1x, Double point1y, Double point1z, Double point2x, Double point2y, Double point2z) {
		return this.intersectionWith(new LineSegment3DExpression(point1x, point1y, point1z, point2x, point2y, point1z));
	}

	/**
	 * Returns an expression providing the point of intersection between this line
	 * segment and the line segment formed from the two coordinates provided.
	 *
	 * @param coord1 the first point of the line segment to compare against
	 * @param coord2 the last point of the line segment to compare against
	 * @return a Point3DExpression
	 */
	public Point3DExpression intersectionWith(Coordinate coord1, Coordinate coord2) {
		return this.intersectionWith(new LineSegment3DExpression(coord1, coord2));
	}

	/**
	 * Returns an expression providing the point of intersection between this line
	 * segment and the line segment provided.
	 *
	 * @param lineString the line segment to compare against
	 * @return a Point3DExpression
	 */
	public Point3DExpression intersectionWith(LineSegmentZ lineString) {
		return this.intersectionWith(new LineSegment3DExpression(lineString));
	}

	/**
	 * Returns an expression providing the point of intersection between this line
	 * segment and the {@link LineSegment3DResult}/{@link LineSegment3DExpression}
	 * provided.
	 *
	 * @param crossingLine the line segment to compare against
	 * @return a Point3DExpression
	 */
	public Point3DExpression intersectionWith(LineSegment3DResult crossingLine) {
		return new Point3DExpression(new LineSegmentLineSegmentWithPointResult(this, new LineSegment3DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment3DIntersectionPointWithLineSegment3DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	@Override
	public DBLineSegment3D asExpressionColumn() {
		return new DBLineSegment3D(this);
	}

	private static abstract class LineSegmentLineSegmentWithBooleanResult extends BooleanExpression {

		private LineSegment3DExpression first;
		private LineSegment3DExpression second;
		private boolean requiresNullProtection;

		LineSegmentLineSegmentWithBooleanResult(LineSegment3DExpression first, LineSegment3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment3DExpression getFirst() {
			return first;
		}

		LineSegment3DExpression getSecond() {
			return second;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public LineSegmentLineSegmentWithBooleanResult copy() {
			LineSegmentLineSegmentWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			if (second != null) {
				hashSet.addAll(second.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class LineSegmentLineSegmentWithPointResult extends Point3DExpression {

		private LineSegment3DExpression first;
		private LineSegment3DExpression second;
		private boolean requiresNullProtection;

		LineSegmentLineSegmentWithPointResult(LineSegment3DExpression first, LineSegment3DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment3DExpression getFirst() {
			return first;
		}

		LineSegment3DExpression getSecond() {
			return second;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public LineSegmentLineSegmentWithPointResult copy() {
			LineSegmentLineSegmentWithPointResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			if (second != null) {
				hashSet.addAll(second.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator() || second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class LineSegmentWithNumberResult extends NumberExpression {

		private LineSegment3DExpression first;
//		private Point3DExpression second;
		private boolean requiresNullProtection;

		LineSegmentWithNumberResult(LineSegment3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment3DExpression getFirst() {
			return first;
		}

//		Point3DExpression getSecond() {
//			return second;
//		}
		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public LineSegmentWithNumberResult copy() {
			LineSegmentWithNumberResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class LineSegmentWithBooleanResult extends BooleanExpression {

		private LineSegment3DExpression first;
//		private Point3DExpression second;
		private boolean requiresNullProtection;

		LineSegmentWithBooleanResult(LineSegment3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment3DExpression getFirst() {
			return first;
		}

//		Point3DExpression getSecond() {
//			return second;
//		}
		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public LineSegmentWithBooleanResult copy() {
			LineSegmentWithBooleanResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class LineSegmentWithStringResult extends StringExpression {

		private LineSegment3DExpression first;
		private boolean requiresNullProtection;

		LineSegmentWithStringResult(LineSegment3DExpression first) {
			this.first = first;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment3DExpression getFirst() {
			return first;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public LineSegmentWithStringResult copy() {
			LineSegmentWithStringResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

	private static abstract class LineSegmentWithGeometry3DResult extends Polygon3DExpression {

		private LineSegment3DExpression first;
//		private Point3DExpression second;
		private boolean requiresNullProtection;

		LineSegmentWithGeometry3DResult(LineSegment3DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment3DExpression getFirst() {
			return first;
		}

//		Point3DExpression getSecond() {
//			return second;
//		}
		@Override
		public final String toSQLString(DBDatabase db) {
			if (this.getIncludesNull()) {
				return BooleanExpression.isNull(first).toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public LineSegmentWithGeometry3DResult copy() {
			LineSegmentWithGeometry3DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.first = first.copy();
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<>();
			if (first != null) {
				hashSet.addAll(first.getTablesInvolved());
			}
//			if (second != null) {
//				hashSet.addAll(second.getTablesInvolved());
//			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			return first.isAggregator();//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

}
