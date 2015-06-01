/*
 * Copyright 2015 greg.
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
import nz.co.gregs.dbvolution.results.Line2DResult;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBLine2D;

/**
 * Represents SQL expressions that are a 2 dimensional path, a series of
 * connected line segments with X and Y coordinates.
 *
 *
 *
 * @author Gregory Graham
 */
public class Line2DExpression implements Line2DResult, EqualComparable<Line2DResult>, SpatialExpression {

	private Line2DResult innerLineString;
	private boolean nullProtectionRequired;

	protected Line2DExpression() {
	}

	public Line2DExpression(Line2DResult value) {
		initInnerLine(value, value);
	}

	protected final void initInnerLine(Object original, Line2DResult value) {
		innerLineString = value;
		if (original == null || innerLineString.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public Line2DExpression(LineString line) {
		innerLineString = new DBLine2D(line);
		initInnerLine(line, innerLineString);
	}

	public Line2DExpression(Point... points) {
		GeometryFactory geometryFactory = new GeometryFactory();
		List<Coordinate> coords = new ArrayList<Coordinate>();
		for (Point point : points) {
			coords.add(point.getCoordinate());
		}
		LineString line = geometryFactory.createLineString(coords.toArray(new Coordinate[]{}));
		innerLineString = new DBLine2D(line);
		initInnerLine(points, innerLineString);
	}

	public Line2DExpression(Coordinate... coords) {
		GeometryFactory geometryFactory = new GeometryFactory();
		LineString line = geometryFactory.createLineString(coords);
		innerLineString = new DBLine2D(line);
		initInnerLine(coords, innerLineString);
	}

	public static Line2DExpression value(Point... points) {
		return new Line2DExpression(points);
	}

	public static Line2DExpression value(Coordinate... coords) {
		return new Line2DExpression(coords);
	}

	public static Line2DExpression value(LineString line) {
		return new Line2DExpression(line);
	}

	@Override
	public DBLine2D getQueryableDatatypeForExpressionValue() {
		return new DBLine2D();
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
	public Line2DExpression copy() {
		return new Line2DExpression(innerLineString);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof Line2DExpression) {
			Line2DExpression otherExpr = (Line2DExpression) other;
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
		return new StringExpression(new Line2DExpression.LineFunctionWithStringResult(this) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DAsTextTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().toSQLString(db);
				}
			}
		});
	}

	public BooleanExpression is(LineString rightHandSide) {
		return is(new DBLine2D(rightHandSide));
	}

	public BooleanExpression is(Polygon rightHandSide) {
		return is(rightHandSide.getExteriorRing());
	}

	@Override
	public BooleanExpression is(Line2DResult rightHandSide) {
		return new BooleanExpression(new Line2DExpression.LineLineWithBooleanResult(this, new Line2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return getFirst().stringResult().is(getSecond().stringResult()).toSQLString(db);
				}
			}
		});
	}

	public BooleanExpression isNot(LineString rightHandSide) {
		return isNot(new DBLine2D(rightHandSide));
	}

	public BooleanExpression isNot(Polygon rightHandSide) {
		return isNot(rightHandSide.getExteriorRing());
	}

	public BooleanExpression isNot(Line2DResult rightHandSide) {
		return new BooleanExpression(new Line2DExpression.LineLineWithBooleanResult(this, new Line2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					final DBDefinition defn = db.getDefinition();
					return defn.doLine2DNotEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
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
					return db.getDefinition().doLine2DDimensionTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					return NumberExpression.value(1).toSQLString(db);
				}
			}
		});
	}

	@Override
	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new LineFunctionWithPolygon2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				try {
					return db.getDefinition().doLine2DGetBoundingBoxTransform(getFirst().toSQLString(db));
				} catch (UnsupportedOperationException unsupported) {
					final Line2DExpression first = getFirst();
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
				return db.getDefinition().doLine2DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMinX() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMaxY() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression getMinY() {

		return new NumberExpression(new LineFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Tests whether this line and the line represented by the points ever cross.
	 * 
	 * <p>
	 * Multiple line segments means it may cross at several points, however this method only reports TRUE or FALSE.
	 * 
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection points of these lines
	 *
	 * @param points 
	 * @return a BooleanExpression that will be TRUE if the lines ever cross, otherwise FALSE.
	 */
	public BooleanExpression intersects(Point... points) {
		return this.intersects(value(points));
	}
	
	/**
	 * Tests whether this line and the line represented by the coordinates ever cross.
	 * 
	 * <p>
	 * Multiple line segments means it may cross at several points, however this method only reports TRUE or FALSE.
	 * 
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection points of these lines
	 *
	 * @param coords 
	 * @return a BooleanExpression that will be TRUE if the lines ever cross, otherwise FALSE.
	 */
	public BooleanExpression intersects(Coordinate... coords) {
		return this.intersects(value(coords));
	}
	
	/**
	 * Tests whether this line and the other line ever cross.
	 * 
	 * <p>
	 * Multiple line segments means it may cross at several points, however this method only reports TRUE or FALSE.
	 * 
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection points of these lines
	 *
	 * @param lineString 
	 * @return a BooleanExpression that will be TRUE if the lines ever cross, otherwise FALSE.
	 */
	public BooleanExpression intersects(LineString lineString) {
		return this.intersects(value(lineString));
	}
	
	/**
	 * Tests whether this line and the other line ever cross.
	 * 
	 * <p>
	 * Multiple line segments means it may cross at several points, however this method only reports TRUE or FALSE.
	 * 
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection points of these lines
	 *
	 * @param crossingLine
	 * @return a BooleanExpression that will be TRUE if the lines ever cross, otherwise FALSE.
	 */
	public BooleanExpression intersects(Line2DResult crossingLine) {
		return new BooleanExpression(new LineLineWithBooleanResult(this, new Line2DExpression(crossingLine)) {
			
			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DIntersectsLine2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}
	
	public Point2DExpression intersectionWith(Point... crossingLine) {
		return intersectionWith(value(crossingLine));
	}
	public Point2DExpression intersectionWith(Coordinate... crossingLine) {
		return intersectionWith(value(crossingLine));
	}
	public Point2DExpression intersectionWith(LineString crossingLine) {
		return intersectionWith(value(crossingLine));
	}
	/**
	 * Find a point where this line and the other line cross.
	 * 
	 * <p>
	 * Multiple line segments means it may cross at several points, however this method only reports the first point found.
	 * 
	 * <p>
	 * Use {@link #intersectionPoints(Line2DResult)} to find the intersection points of these lines
	 *
	 * @param crossingLine
	 * @return a BooleanExpression that will be TRUE if the lines ever cross, otherwise FALSE.
	 */
	public Point2DExpression intersectionWith(Line2DResult crossingLine) {
		return new Point2DExpression(new LineLineWithPoint2DResult(this, new Line2DExpression(crossingLine)) {
			
			@Override
			protected String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doLine2DIntersectionPointWithLine2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}
	
	private static abstract class LineLineWithBooleanResult extends BooleanExpression {

		private Line2DExpression first;
		private Line2DExpression second;
		private boolean requiresNullProtection;

		LineLineWithBooleanResult(Line2DExpression first, Line2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

		Line2DExpression getSecond() {
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

	private static abstract class LineLineWithPoint2DResult extends Point2DExpression {

		private Line2DExpression first;
		private Line2DExpression second;
		private boolean requiresNullProtection;

		LineLineWithPoint2DResult(Line2DExpression first, Line2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
			return first;
		}

		Line2DExpression getSecond() {
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
		public LineLineWithPoint2DResult copy() {
			LineLineWithPoint2DResult newInstance;
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

		private Line2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineFunctionWithNumberResult(Line2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
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

		private Line2DExpression first;
		private boolean requiresNullProtection;

		LineFunctionWithStringResult(Line2DExpression first) {
			this.first = first;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
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

	private static abstract class LineFunctionWithPolygon2DResult extends Polygon2DExpression {

		private Line2DExpression first;
//		private Point2DExpression second;
		private boolean requiresNullProtection;

		LineFunctionWithPolygon2DResult(Line2DExpression first) {
			this.first = first;
//			this.second = second;
			if (this.first == null) {// || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Line2DExpression getFirst() {
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
		public LineFunctionWithPolygon2DResult copy() {
			LineFunctionWithPolygon2DResult newInstance;
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
