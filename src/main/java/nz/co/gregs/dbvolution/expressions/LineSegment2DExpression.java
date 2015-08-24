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
import nz.co.gregs.dbvolution.results.LineSegment2DResult;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLineSegment2D;

/**
 * Represents expressions that produce a geometry consisting of 2 points and
 * representing a line from the first point to the second.
 *
 * @author gregory.graham
 */
public class LineSegment2DExpression implements LineSegment2DResult, EqualComparable<LineSegment2DResult>, Spatial2DExpression {

	private LineSegment2DResult innerLineString;
	private boolean nullProtectionRequired;

	/**
	 * Default constructor.
	 *
	 */
	protected LineSegment2DExpression() {
	}

	/**
	 * Create a LineSegment2D expression encapsulating the value supplied.
	 *
	 * @param value
	 */
	public LineSegment2DExpression(LineSegment2DResult value) {
		initInnerLine(value, value, value);
	}

	private void initInnerLine(Object original1, Object original2, LineSegment2DResult value) {
		innerLineString = value;
		if (original1 == null || original2 == null || innerLineString.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a LineSegment2D expression encapsulating the value supplied.
	 *
	 * @param line
	 */
	public LineSegment2DExpression(LineSegment line) {
		innerLineString = new DBLineSegment2D(line);
		initInnerLine(line, line, innerLineString);
	}

	/**
	 * Create a LineSegment2D expression encapsulating the values supplied.
	 *
	 * @param point1x
	 * @param point1y
	 * @param point2x
	 * @param point2y
	 */
	public LineSegment2DExpression(Double point1x, Double point1y, Double point2x, Double point2y) {
		LineSegment line = null;
		if (point1x != null && point1y != null && point2x != null && point2y != null) {
			line = new LineSegment(point1x, point1y, point2x, point2y);
			innerLineString = new DBLineSegment2D(line);
		} else {
			innerLineString = new DBLineSegment2D(line);
			nullProtectionRequired = true;
		}
	}

	/**
	 * Create a LineSegment2D expression encapsulating the value supplied.
	 *
	 * @param point1
	 * @param point2
	 *
	 */
	public LineSegment2DExpression(Point point1, Point point2) {
		LineSegment line = null;
		if (point1 != null && point2 != null) {
			line = new LineSegment(point1.getCoordinate(), point2.getCoordinate());
			innerLineString = new DBLineSegment2D(line);
		} else {
			innerLineString = new DBLineSegment2D(line);
		}
		initInnerLine(point1, point2, innerLineString);
	}

	/**
	 * Create a LineSegment2D expression encapsulating the value supplied.
	 *
	 * @param coord1
	 * @param coord2
	 */
	public LineSegment2DExpression(Coordinate coord1, Coordinate coord2) {
		LineSegment line = null;
		if (coord1 != null && coord2 != null) {
			line = new LineSegment(coord1, coord2);
			innerLineString = new DBLineSegment2D(line);
		} else {
			innerLineString = new DBLineSegment2D(line);
		}
		initInnerLine(coord1, coord2, innerLineString);
	}

	/**
	 * Create an expression for the line segment created from the 2 points.
	 *
	 * @param point1
	 * @param point2
	 * @return a LineSegment2D expression
	 */
	public static LineSegment2DExpression value(Point point1, Point point2) {
		return new LineSegment2DExpression(point1, point2);
	}

	/**
	 * Create an expression for the line segment created from the 2 coordinates.
	 *
	 * @param coord1
	 * @param coord2
	 * @return a LineSegment2D expression
	 */
	public static LineSegment2DExpression value(Coordinate coord1, Coordinate coord2) {
		return new LineSegment2DExpression(coord1, coord2);
	}

	/**
	 * Create an expression for the line segment created by combining the 4
	 * numbers into 2 points.
	 *
	 * @param x1
	 * @param y1
	 * @param x2
	 * @param y2
	 * @return a LineSegment2D expression
	 */
	public static LineSegment2DExpression value(Double x1, Double y1, Double x2, Double y2) {
		return new LineSegment2DExpression(new Coordinate(x1, y1), new Coordinate(x2, y2));
	}

	/**
	 * Create an expression for the line segment created from the 2 points.
	 *
	 * @param line
	 * @return a LineSegment2D expression
	 */
	public static LineSegment2DExpression value(LineSegment line) {
		return new LineSegment2DExpression(line);
	}

	@Override
	public DBLineSegment2D getQueryableDatatypeForExpressionValue() {
		return new DBLineSegment2D();
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
	public LineSegment2DExpression copy() {
		return new LineSegment2DExpression(innerLineString);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof LineSegment2DExpression) {
			LineSegment2DExpression otherExpr = (LineSegment2DExpression) other;
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
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
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
	 * Convert this LineSegment2D to a String representation based on the Well
	 * Known Text (WKT) format.
	 *
	 * @return a StringExpression representing this spatial value.
	 */
	public StringExpression stringResult() {
		return new StringExpression(new LineSegmentWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment2DAsTextTransform(getFirst().toSQLString(db));
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
	 * @param rightHandSide
	 * @return a BooleanExpression
	 */
	public BooleanExpression is(LineSegment rightHandSide) {
		return is(new DBLineSegment2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(LineSegment2DResult rightHandSide) {
		return new BooleanExpression(new LineSegmentLineSegmentWithBooleanResult(this, new LineSegment2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
	 * @param rightHandSide
	 * @return a BooleanExpression returning TRUE if the two line segments are
	 * different, otherwise FALSE.
	 */
	public BooleanExpression isNot(LineSegment rightHandSide) {
		return isNot(new DBLineSegment2D(rightHandSide));
	}

	@Override
	public BooleanExpression isNot(LineSegment2DResult rightHandSide) {
		return is(rightHandSide).not();
	}

	@Override
	public NumberExpression measurableDimensions() {
		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment2DDimensionTransform(getFirst().toSQLString(db));
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
					return db.getDefinition().doLineSegment2DSpatialDimensionsTransform(getFirst().toSQLString(db));
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
					return db.getDefinition().doLineSegment2DHasMagnitudeTransform(getFirst().toSQLString(db));
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
					return db.getDefinition().doLineSegment2DGetMagnitudeTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return nullExpression().toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new LineSegmentWithGeometry2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLineSegment2DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final LineSegment2DExpression first = getFirst();
					final NumberExpression maxX = first.getMaxX();
					final NumberExpression maxY = first.getMaxY();
					final NumberExpression minX = first.getMinX();
					final NumberExpression minY = first.getMinY();
					return Polygon2DExpression.value(
							Point2DExpression.value(minX, minY),
							Point2DExpression.value(maxX, minY),
							Point2DExpression.value(maxX, maxY),
							Point2DExpression.value(minX, maxY),
							Point2DExpression.value(minX, minY))
							.toSQLString(db);
				}
			}
		});
	}

	public NumberExpression getMaxX() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMinX() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMaxY() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMinY() {

		return new NumberExpression(new LineSegmentWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Tests whether this line segment and the line segment represented by the points ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.Point, com.vividsolutions.jts.geom.Point) } to find the intersection
	 * point.
	 *
	 * @param point1
	 * @param point2
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Point point1, Point point2) {
		return this.intersects(new LineSegment2DExpression(point1, point2));
	}

	/**
	 * Tests whether this line segment and the line segment represented by the coordinates ever
	 * cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.Coordinate, com.vividsolutions.jts.geom.Coordinate) } to find the intersection
	 * point.
	 *
	 * @param coord1
	 * @param coord2
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(Coordinate coord1, Coordinate coord2) {
		return this.intersects(new LineSegment2DExpression(coord1, coord2));
	}

	/**
	 * Tests whether this line segment and the other line segment ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(com.vividsolutions.jts.geom.LineSegment) } to find the intersection
	 * point.
	 *
	 * @param linesegment
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineSegment linesegment) {
		return this.intersects(new LineSegment2DExpression(linesegment));
	}

	/**
	 * Tests whether this line segment and the other line segment ever cross.
	 *
	 * <p>
	 * Use {@link #intersectionWith(nz.co.gregs.dbvolution.results.LineSegment2DResult) } to find the intersection
	 * point.
	 *
	 * @param crossingLine
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineSegment2DResult crossingLine) {
		return new BooleanExpression(new LineSegmentLineSegmentWithBooleanResult(this, new LineSegment2DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DIntersectsLineSegment2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Returns an expression providing the point of intersection between
	 * this line segment and the line segment formed from the two points provided.
	 *
	 * @param point1
	 * @param point2
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(Point point1, Point point2) {
		return this.intersectionWith(new LineSegment2DExpression(point1, point2));
	}

	/**
	 * Returns an expression providing the point of intersection between
	 * this line segment and the line segment formed from the four ordinates provided.
	 *
	 * @param point1x 
	 * @param point1y 
	 * @param point2x  
	 * @param point2y  
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(Double point1x, Double point1y, Double point2x, Double point2y) {
		return this.intersectionWith(new LineSegment2DExpression(point1x, point1y, point2x, point2y));
	}

	/**
	 * Returns an expression providing the point of intersection between
	 * this line segment and the line segment formed from the two coordinates provided.
	 *
	 * @param coord1 
	 * @param coord2 
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(Coordinate coord1, Coordinate coord2) {
		return this.intersectionWith(new LineSegment2DExpression(coord1, coord2));
	}

	/**
	 * Returns an expression providing the point of intersection between
	 * this line segment and the line segment provided.
	 *
	 * @param lineString 
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(LineSegment lineString) {
		return this.intersectionWith(new LineSegment2DExpression(lineString));
	}

	/**
	 * Returns an expression providing the point of intersection between
	 * this line segment and the {@link LineSegment2DResult}/{@link LineSegment2DExpression} provided.
	 *
	 * @param crossingLine 
	 * @return a Point2DExpression
	 */
	public Point2DExpression intersectionWith(LineSegment2DResult crossingLine) {
		return new Point2DExpression(new LineSegmentLineSegmentWithPointResult(this, new LineSegment2DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DIntersectionPointWithLineSegment2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	private static abstract class LineSegmentLineSegmentWithBooleanResult extends BooleanExpression {

		private LineSegment2DExpression first;
		private LineSegment2DExpression second;
		private boolean requiresNullProtection;

		LineSegmentLineSegmentWithBooleanResult(LineSegment2DExpression first, LineSegment2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
			return first;
		}

		LineSegment2DExpression getSecond() {
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
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
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

	private static abstract class LineSegmentLineSegmentWithPointResult extends Point2DExpression {

		private LineSegment2DExpression first;
		private LineSegment2DExpression second;
		private boolean requiresNullProtection;

		LineSegmentLineSegmentWithPointResult(LineSegment2DExpression first, LineSegment2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
			return first;
		}

		LineSegment2DExpression getSecond() {
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
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
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

		private LineSegment2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineSegmentWithNumberResult(LineSegment2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
			return first;
		}

//		Point2DExpression getSecond() {
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
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
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

		private LineSegment2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineSegmentWithBooleanResult(LineSegment2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
			return first;
		}

//		Point2DExpression getSecond() {
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
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
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

		private LineSegment2DExpression first;
		private boolean requiresNullProtection;

		LineSegmentWithStringResult(LineSegment2DExpression first) {
			this.first = first;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
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
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
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

	private static abstract class LineSegmentWithGeometry2DResult extends Polygon2DExpression {

		private LineSegment2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineSegmentWithGeometry2DResult(LineSegment2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		LineSegment2DExpression getFirst() {
			return first;
		}

//		Point2DExpression getSecond() {
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
		public LineSegmentWithGeometry2DResult copy() {
			LineSegmentWithGeometry2DResult newInstance;
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
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
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
