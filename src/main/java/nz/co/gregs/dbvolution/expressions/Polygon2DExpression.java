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

//import com.vividsolutions.jts.geom.Geometry;
import nz.co.gregs.dbvolution.results.EqualComparable;
import nz.co.gregs.dbvolution.results.Point2DResult;
import nz.co.gregs.dbvolution.results.Polygon2DResult;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPoint2D;
import nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2D;

/**
 *
 * @author gregorygraham
 */
public class Polygon2DExpression implements Polygon2DResult, EqualComparable<Polygon2DResult> {

	private Polygon2DResult innerGeometry;
	private boolean nullProtectionRequired;

	protected Polygon2DExpression() {
	}

	public Polygon2DExpression(Polygon2DResult value) {
		innerGeometry = value;
		if (value == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public Polygon2DExpression(Polygon geometry) {
		innerGeometry = new DBPolygon2D(geometry);
		if (geometry == null || innerGeometry.getIncludesNull()) {
			nullProtectionRequired = true;
		}
	}

	public static Polygon2DExpression value(Polygon geometry) {
		return new Polygon2DExpression(geometry);
	}

	public static Polygon2DExpression value(Point2DExpression... pointExpressions) {
		return Polygon2DExpression.polygon2DFromPoint2DArray(pointExpressions);
	}

	@Override
	public DBPolygon2D getQueryableDatatypeForExpressionValue() {
		return new DBPolygon2D();
	}

	@Override
	public String toSQLString(DBDatabase db) {
		if (innerGeometry == null) {
			return db.getDefinition().getNull();
		} else {
			return innerGeometry.toSQLString(db);
		}
	}

	@Override
	public Polygon2DExpression copy() {
		return new Polygon2DExpression(innerGeometry);
	}

	@Override
	public boolean isAggregator() {
		return innerGeometry == null ? false : innerGeometry.isAggregator();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		HashSet<DBRow> hashSet = new HashSet<DBRow>();
		if (innerGeometry != null) {
			hashSet.addAll(innerGeometry.getTablesInvolved());
		}
		return hashSet;
	}

	@Override
	public boolean isPurelyFunctional() {
		return innerGeometry == null ? true : innerGeometry.isPurelyFunctional();
	}

	@Override
	public boolean getIncludesNull() {
		return nullProtectionRequired;
	}

	public BooleanExpression intersects(Polygon rightHandSide) {
		return intersects(new DBPolygon2D(rightHandSide));
	}

	public BooleanExpression intersects(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DIntersectsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public static Polygon2DExpression polygon2DFromPoint2DArray(Point... points) {
		List<Point2DExpression> exprs = new ArrayList<Point2DExpression>();
		for (Point point : points) {
			exprs.add(Point2DExpression.value(point));
		}
		return polygon2DFromPoint2DArray(exprs.toArray(new Point2DExpression[]{}));
	}

	public static Polygon2DExpression polygon2DFromPoint2DArray(Point2DExpression... pointExpressions) {
		return new Polygon2DExpression(new Point2dArrayFunctionWithPolygon2DResult(pointExpressions) {

			@Override
			protected String doExpressionTransform(DBDatabase db) {
				Point2DExpression[] allPoints = getAllPoints();
				List<String> pointSQL = new ArrayList<String>();
				for (Point2DExpression pointExpr : allPoints) {
					pointSQL.add(pointExpr.toSQLString(db));
				}
				try {
					return db.getDefinition().doPoint2DArrayToPolygon2DTransform(pointSQL);
				} catch (UnsupportedOperationException ex) {
					StringExpression newPolygon = StringExpression.value("POLYGON((");
					String separator = "";

					for (Point2DExpression point : allPoints) {
						newPolygon.append(separator).append(point.stringResult());
						separator = ",";
					}
					newPolygon.append(separator).append(allPoints[0].stringResult());
					return newPolygon.toSQLString(db);
				}
			}
		});
	}

	public BooleanExpression is(Polygon rightHandSide) {
		return is(new DBPolygon2D(rightHandSide));
	}

	@Override
	public BooleanExpression is(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DEqualsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression contains(Point rightHandSide) {
		return contains(new Point2DExpression(rightHandSide));
	}

	public BooleanExpression contains(Point2DResult rightHandSide) {
		return contains(new Point2DExpression(rightHandSide));
	}

	public BooleanExpression contains(Point2DExpression rightHandSide) {
		return new BooleanExpression(new PolygonPointWithBooleanResult(this, rightHandSide) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DContainsPoint2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression contains(Polygon rightHandSide) {
		return contains(new DBPolygon2D(rightHandSide));
	}

	public BooleanExpression contains(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DContainsPolygon2DTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression doesNotIntersect(Polygon rightHandSide) {
		return doesNotIntersect(new DBPolygon2D(rightHandSide));
	}

	public BooleanExpression doesNotIntersect(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DDoesNotIntersectTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression overlaps(Polygon rightHandSide) {
		return overlaps(new DBPolygon2D(rightHandSide));
	}

	public BooleanExpression overlaps(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DOverlapsTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	/**
	 * Tests whether the polygons touch.
	 *
	 * <p>
	 * Checks that a) the polygons have at least on point in common and b) that
	 * their interiors do not overlap.
	 *
	 * @param rightHandSide
	 * @return BooleanExpression that returns TRUE if and only if the polygons
	 * touch without overlapping
	 */
	public BooleanExpression touches(Polygon rightHandSide) {
		return touches(new DBPolygon2D(rightHandSide));
	}

	/**
	 * Tests whether the polygons touch.
	 *
	 * <p>
	 * Checks that a) the polygons have at least on point in common and b) that
	 * their interiors do not overlap.
	 *
	 * @param rightHandSide
	 * @return BooleanExpression that returns TRUE if and only if the polygons
	 * touch without overlapping
	 */
	public BooleanExpression touches(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DTouchesTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public BooleanExpression within(Polygon rightHandSide) {
		return within(new DBPolygon2D(rightHandSide));
	}

	public BooleanExpression within(Polygon2DResult rightHandSide) {
		return new BooleanExpression(new PolygonPolygonWithBooleanResult(this, new Polygon2DExpression(rightHandSide)) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DWithinTransform(getFirst().toSQLString(db), getSecond().toSQLString(db));
			}
		});
	}

	public NumberExpression dimension() {
		return new NumberExpression(new PolygonFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetDimensionTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression area() {
		return new NumberExpression(new PolygonFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetAreaTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression maxX() {
		return new NumberExpression(new PolygonFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetMaxXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression minX() {
		return new NumberExpression(new PolygonFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetMinXTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression maxY() {
		return new NumberExpression(new PolygonFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetMaxYTransform(getFirst().toSQLString(db));
			}
		});
	}

	public NumberExpression minY() {
		return new NumberExpression(new PolygonFunctionWithNumberResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetMinYTransform(getFirst().toSQLString(db));
			}
		});
	}

	public Polygon2DExpression boundingBox() {
		return new Polygon2DExpression(new Polygon2DFunctionWithPolygon2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetBoundingBoxTransform(getFirst().toSQLString(db));
			}
		});
	}

	/**
	 * Return a Line2DExpression representing a line drawn around the outside of
	 * the Polygon2D.
	 *
	 * <p>
	 * The line is coincident with the edge of the polygon but it does not contain
	 * any points within the polygon as it is only a line.
	 *
	 * @return
	 */
	public Line2DExpression exteriorRing() {
		Line2DExpression exteriorRingExpr = new Line2DExpression(new Polygon2DFunctionWithLine2DResult(this) {

			@Override
			public String doExpressionTransform(DBDatabase db) {
				return db.getDefinition().doPolygon2DGetExteriorRingTransform(getFirst().toSQLString(db));
			}
		});
//		return this.dimension().is(2).ifThenElse(exteriorRingExpr, this);
		return exteriorRingExpr;
	}

	public BooleanExpression isNot(Polygon geometry) {
		return this.is(geometry).not();
	}

	public BooleanExpression isNot(Polygon2DResult geometry) {
		return this.is(geometry).not();
	}

	private static abstract class PolygonPolygonWithBooleanResult extends BooleanExpression {

		private Polygon2DExpression first;
		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		PolygonPolygonWithBooleanResult(Polygon2DExpression first, Polygon2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

		Polygon2DResult getSecond() {
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
		public PolygonPolygonWithBooleanResult copy() {
			PolygonPolygonWithBooleanResult newInstance;
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
	private static abstract class PolygonPointWithBooleanResult extends BooleanExpression {

		private Polygon2DExpression first;
		private Point2DExpression second;
		private boolean requiresNullProtection;

		PolygonPointWithBooleanResult(Polygon2DExpression first, Point2DExpression second) {
			this.first = first;
			this.second = second;
			if (this.second == null || this.second.getIncludesNull()) {
				this.requiresNullProtection = true;
			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

		Point2DExpression getSecond() {
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
		public PolygonPointWithBooleanResult copy() {
			PolygonPointWithBooleanResult newInstance;
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

	private static abstract class PolygonFunctionWithNumberResult extends NumberExpression {

		private Polygon2DExpression first;
//		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		PolygonFunctionWithNumberResult(Polygon2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

//		Polygon2DResult getSecond() {
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
		public PolygonFunctionWithNumberResult copy() {
			PolygonFunctionWithNumberResult newInstance;
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

	private static abstract class Polygon2DFunctionWithPolygon2DResult extends Polygon2DExpression {

		private Polygon2DExpression first;
//		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithPolygon2DResult(Polygon2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

//		Polygon2DResult getSecond() {
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
		public Polygon2DFunctionWithPolygon2DResult copy() {
			Polygon2DFunctionWithPolygon2DResult newInstance;
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

	private static abstract class Polygon2DFunctionWithLine2DResult extends Line2DExpression {

		private Polygon2DExpression first;
//		private Polygon2DExpression second;
		private boolean requiresNullProtection;

		Polygon2DFunctionWithLine2DResult(Polygon2DExpression first) {
			this.first = first;
//			this.second = second;
//			if (this.second == null || this.second.getIncludesNull()) {
//				this.requiresNullProtection = true;
//			}
		}

		Polygon2DExpression getFirst() {
			return first;
		}

//		Polygon2DResult getSecond() {
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
		public Polygon2DFunctionWithLine2DResult copy() {
			Polygon2DFunctionWithLine2DResult newInstance;
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

	private static abstract class Point2dArrayFunctionWithPolygon2DResult extends Polygon2DExpression {

		private Point2DExpression[] allPoints;
		private boolean requiresNullProtection;

		Point2dArrayFunctionWithPolygon2DResult(Point2DExpression... all) {
			this.allPoints = all;
			for (Point2DExpression all1 : all) {
				if (all1.getIncludesNull()) {
					this.requiresNullProtection = true;
				}
			}
		}

		Point2DExpression[] getAllPoints() {
			return allPoints;
		}

		@Override
		public final String toSQLString(DBDatabase db) {
			BooleanExpression isNull = BooleanExpression.trueExpression();
			if (this.getIncludesNull()) {
				for (Point2DExpression allPoint : allPoints) {
					isNull = isNull.or(BooleanExpression.isNull(allPoint));
				}
				return isNull.toSQLString(db);
			} else {
				return doExpressionTransform(db);
			}
		}

		@Override
		public Point2dArrayFunctionWithPolygon2DResult copy() {
			Point2dArrayFunctionWithPolygon2DResult newInstance;
			try {
				newInstance = getClass().newInstance();
			} catch (InstantiationException ex) {
				throw new RuntimeException(ex);
			} catch (IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
			newInstance.allPoints = Arrays.copyOf(allPoints, allPoints.length);
//			newInstance.second = second.copy();
			return newInstance;
		}

		protected abstract String doExpressionTransform(DBDatabase db);

		@Override
		public Set<DBRow> getTablesInvolved() {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			if (allPoints != null) {
				for (Point2DExpression point : allPoints) {
					hashSet.addAll(point.getTablesInvolved());
				}
			}
			return hashSet;
		}

		@Override
		public boolean isAggregator() {
			boolean aggregator = false;
			for (Point2DExpression allPoint : allPoints) {
				aggregator |= allPoint.isAggregator();
			}
			return aggregator;//|| second.isAggregator();
		}

		@Override
		public boolean getIncludesNull() {
			return requiresNullProtection;
		}
	}

}
