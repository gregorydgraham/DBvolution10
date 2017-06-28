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
package nz.co.gregs.dbvolution.datatypes.spatial3D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.*;
import nz.co.gregs.dbvolution.expressions.MultiPoint3DExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.MultiPoint3DResult;

/**
 * Represents database columns and values that are list of 3 dimensional points:
 * a MULTIPOINT.
 *
 * <p>
 * Use DBPoint3D when the column is a 3 dimensional {@code Point},
 * {@code ST_Point}, or {@code GEOMETRY} that represents a point.
 *
 * <p>
 Generally DBMultiPoint3D is declared inside your DBRow sub-class as:
 {@code @DBColumn public DBMultiPoint3D myPointColumn = new DBMultiPoint3D();}
 *
 *
 * @author Gregory Graham
 */
public class DBMultiPoint3D extends QueryableDatatype<MultiPoint> implements MultiPoint3DResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Create an unset undefined DBPoint3D object to represent a Point column or
	 * value.
	 *
	 */
	public DBMultiPoint3D() {
	}

	/**
	 * Create a DBPoint2D object to represent a Point column or
	 * value using the value supplied.
	 *
	 * @param points the collection of points that are the value of this DBMultiPoint2D
	 */
	public DBMultiPoint3D(MultiPoint points) {
		super(points);
	}

	/**
	 * Set the value of this DBPoint2D to the {@link Point} specified.
	 *
	 * <p>
	 * Set values are used to add the value to the database. Without a set value
	 * the database entry will be NULL.
	 *
	 * @param point the value to be set in the database.
	 */
	public void setValue(MultiPoint point) {
		setLiteralValue(point);
	}

	/**
	 * Set the value of this DBPoint2D to the {@link Coordinate coordinates} specified.
	 *
	 * <p>
	 * Set values are used to add the value to the database. Without a set value
	 * the database entry will be NULL.
	 *
	 * @param coordinates the value to be set in the database.
	 */
	public void setValue(Coordinate... coordinates) {
		GeometryFactory geometryFactory = new GeometryFactory();
		MultiPoint mpoint = geometryFactory.createMultiPoint(coordinates);
		this.setValue(mpoint);
	}

	/**
	 * Set the value of this DBPoint2D to the {@link Point points} specified.
	 *
	 * <p>
	 * Set values are used to add the value to the database. Without a set value
	 * the database entry will be NULL.
	 *
	 * @param points the value to be set in the database.
	 */
	public void setValue(Point... points) {
		GeometryFactory geometryFactory = new GeometryFactory();
		MultiPoint mpoint = geometryFactory.createMultiPoint(points);
		this.setValue(mpoint);
	}

	@Override
	@SuppressWarnings("unchecked")
	public MultiPoint getValue() {
		if (!isDefined() || isNull()) {
			return null;
		} else {
			final Object literalValue = getLiteralValue();
			if (literalValue == null) {
				return null;
			} else if (literalValue instanceof MultiPoint) {
				return (MultiPoint) literalValue;
			}else {
				throw new DBRuntimeException("Unable to convert value to NULL or JTS MultiPoint: Please check that the value is NULL or an appropiate MULTIPOINT type value for this database");
			}
		}
	}

	/**
	 * Convert the value of this object to a JTS {@link Point}.
	 *
	 * <p>
	 * NULL is valid result from this method.
	 *
	 * @return the set value of this object as a JTS Point object.
	 */
	public MultiPoint jtsMultiPointValue() {
		return getValue();
	}

	/**
	 * Create a DBPoint3D with the column expression specified.
	 *
	 * <p>
	 * When retrieving this object from the database the expression will be
	 * evaluated to provide the value.
	 *
	 * @param columnExpression
	 */
	public DBMultiPoint3D(MultiPoint3DExpression columnExpression) {
		super(columnExpression);
	}

	/**
	 * Create DBpoint3D and set it's value to the JTS {@link  Point} provided.
	 *
	 * <p>
	 * Equivalent to {code point3D = new DBPoint3D(); point3D.setValue(aPoint);}
	 *
	 * @param points
	 */
	public DBMultiPoint3D(Point... points) {
		super(new MultiPoint(points, new GeometryFactory()));
	}

	@Override
	public String getSQLDatatype() {
		return " MULTIPOINT ";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		MultiPoint points = getValue();
		if (points == null) {
			return db.getDefinition().getNull();
		} else {
			String str = db.getDefinition().transformMultiPoint2DToDatabaseMultiPoint2DValue(points);
			return str;
		}
	}

	@Override
	protected MultiPoint getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException, IncorrectGeometryReturnedForDatatype {

		MultiPoint point = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				point = database.getDefinition().transformDatabaseMultiPoint2DValueToJTSMultiPoint(string);
			} catch (ParseException ex) {
				Logger.getLogger(DBPoint3D.class.getName()).log(Level.SEVERE, null, ex);
				throw new ParsingSpatialValueException(fullColumnName, string,ex);
			}
			return point;
		}
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public boolean getIncludesNull() {
		return false;
	}

	@Override
	public StringExpression stringResult() {
		return MultiPoint3DExpression.value(this).stringResult();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
}
