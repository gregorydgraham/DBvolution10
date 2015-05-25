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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.Point;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLineSegment2D;

/**
 *
 * @author gregory.graham
 */
public class LineSegment2DExpression implements LineSegment2DResult, EqualComparable<LineSegment2DResult>, SpatialExpression {

	private LineSegment2DResult innerLineString;
	private boolean nullProtectionRequired;

	protected LineSegment2DExpression() {
	}

	public LineSegment2DExpression(LineSegment2DResult value) {
		initInnerLine(value, value, value);
	}

	protected final void initInnerLine(Object original1, Object original2, LineSegment2DResult value) {
		innerLineString = value;
		if (original1 == null || original2 == null || innerLineString.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public LineSegment2DExpression(LineSegment line) {
		innerLineString = new DBLineSegment2D(line);
		initInnerLine(line, line, innerLineString);
	}

	public LineSegment2DExpression(Double point1x, Double point1y, Double point2x, Double point2y) {
		LineSegment line = null;
		if (point1x != null && point1y != null && point2x != null && point2y != null) {
			line = new LineSegment(point1x, point1y, point2x, point2y);
			innerLineString = new DBLineSegment2D(line);
		} else {
			innerLineString = new DBLineSegment2D(line);
			nullProtectionRequired= true;
		}
	}

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

	public static LineSegment2DExpression value(Point point1, Point point2) {
		return new LineSegment2DExpression(point1, point2);
	}

	public static LineSegment2DExpression value(Coordinate coord1, Coordinate coord2) {
		return new LineSegment2DExpression(coord1, coord2);
	}

	public static LineSegment2DExpression value(Double x1, Double y1, Double x2, Double y2) {
		return new LineSegment2DExpression(new Coordinate(x1, y1), new Coordinate(x2, y2));
	}

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

	public StringExpression stringResult() {
		return new StringExpression(new LineFunctionWithStringResult(this) {

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

	public BooleanExpression is(LineSegment rightHandSide) {
		return is(new DBLineSegment2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(LineSegment2DResult rightHandSide) {
		return new BooleanExpression(new LineLineWithBooleanResult(this, new LineSegment2DExpression(rightHandSide)) {

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

	public BooleanExpression isNot(LineSegment rightHandSide) {
		return isNot(new DBLineSegment2D(rightHandSide));
	}

	public BooleanExpression isNot(LineSegment2DResult rightHandSide) {
		return new BooleanExpression(new LineLineWithBooleanResult(this, new LineSegment2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					final DBDefinition defn = db.getDefinition();
					return defn.doLineSegment2DNotEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().is(getSecond().stringResult()).not().toSQLString(db);
				}
			}
		});
	}

	@Override
	public NumberExpression dimension() {
		return new NumberExpression(new LineFunctionWithNumberResult(this) {

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
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new LineFunctionWithGeometry2DResult(this) {

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
							Point2DExpression.value(minX, minY)).toSQLString(db);
				}
			}
		});
	}

	public NumberExpression getMaxX() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMinX() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMaxY() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMinY() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Tests whether this line and the line represented by the points ever cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection
	 * points of these lines
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
	 * Tests whether this line and the line represented by the coordinates ever
	 * cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection
	 * points of these lines
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
	 * Tests whether this line and the other line ever cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection
	 * points of these lines
	 *
	 * @param lineString
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineSegment lineString) {
		return this.intersects(new LineSegment2DExpression(lineString));
	}

	/**
	 * Tests whether this line and the other line ever cross.
	 *
	 * <p>
	 * Multiple line segments means it may cross at several points, however this
	 * method only reports TRUE or FALSE.
	 *
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection
	 * points of these lines
	 *
	 * @param crossingLine
	 * @return a BooleanExpression that will be TRUE if the lines ever cross,
	 * otherwise FALSE.
	 */
	public BooleanExpression intersects(LineSegment2DResult crossingLine) {
		return new BooleanExpression(new LineLineWithBooleanResult(this, new LineSegment2DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DIntersectsLineSegment2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public Point2DExpression intersectionWith(Point point1, Point point2) {
		return this.intersectionWith(new LineSegment2DExpression(point1, point2));
	}

	public Point2DExpression intersectionWith(Double point1x, Double point1y, Double point2x, Double point2y) {
		return this.intersectionWith(new LineSegment2DExpression(point1x, point1y, point2x, point2y));
	}

	public Point2DExpression intersectionWith(Coordinate coord1, Coordinate coord2) {
		return this.intersectionWith(new LineSegment2DExpression(coord1, coord2));
	}

	public Point2DExpression intersectionWith(LineSegment lineString) {
		return this.intersectionWith(new LineSegment2DExpression(lineString));
	}

	public Point2DExpression intersectionWith(LineSegment2DResult crossingLine) {
		return new Point2DExpression(new LineLineWithPointResult(this, new LineSegment2DExpression(crossingLine)) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLineSegment2DIntersectionPointWithLineSegment2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	private static abstract class LineLineWithBooleanResult extends BooleanExpression {

		private LineSegment2DExpression first;
		private LineSegment2DExpression second;
		private boolean requiresNullProtection;

		LineLineWithBooleanResult(LineSegment2DExpression first, LineSegment2DExpression second) {
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
		public LineLineWithBooleanResult copy() {
			LineLineWithBooleanResult newInstance;
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

	private static abstract class LineLineWithPointResult extends Point2DExpression {

		private LineSegment2DExpression first;
		private LineSegment2DExpression second;
		private boolean requiresNullProtection;

		LineLineWithPointResult(LineSegment2DExpression first, LineSegment2DExpression second) {
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
		public LineLineWithPointResult copy() {
			LineLineWithPointResult newInstance;
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

	private static abstract class LineFunctionWithNumberResult extends NumberExpression {

		private LineSegment2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineFunctionWithNumberResult(LineSegment2DExpression first) {
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
		public LineFunctionWithNumberResult copy() {
			LineFunctionWithNumberResult newInstance;
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

	private static abstract class LineFunctionWithStringResult extends StringExpression {

		private LineSegment2DExpression first;
		private boolean requiresNullProtection;

		LineFunctionWithStringResult(LineSegment2DExpression first) {
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
		public LineFunctionWithStringResult copy() {
			LineFunctionWithStringResult newInstance;
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

	private static abstract class LineFunctionWithGeometry2DResult extends Polygon2DExpression {

		private LineSegment2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineFunctionWithGeometry2DResult(LineSegment2DExpression first) {
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
		public LineFunctionWithGeometry2DResult copy() {
			LineFunctionWithGeometry2DResult newInstance;
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
