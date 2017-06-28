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
import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException;
import nz.co.gregs.dbvolution.expressions.Line3DExpression;
import nz.co.gregs.dbvolution.expressions.MultiPoint3DExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.Line3DResult;
import nz.co.gregs.dbvolution.results.MultiPoint3DResult;

/**
 * Represents datatypes and columns that are composed of a series of points
 * connected as a line.
 *
 * <p>
 * Use this type if the database column stores a series of 3-dimensional (that
 * is X, Y and Z points) that are contiguous and open.
 *
 * <p>
 * Alternatives to a DBLine3D are single line segments {@link DBLineSegment3D},
 * infinite lines (#TODO), closed paths (#TODO), and closed
 * paths defining a solid {@link DBPolygon3D}.
 *
 * <p>
 * Common datatypes covered by this type include LINESTRING.
 *
 * <p>
 * Spatial types are not automatically generated during schema extraction so you
 * may need to change some DBString fields.
 *
 * @author gregorygraham
 */
public class DBLine3D extends QueryableDatatype<LineString> implements Line3DResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor.
	 *
	 * Use this method to create the DBLine3D used in your DBRow subclass.
	 *
	 */
	public DBLine3D() {
	}

	/**
	 * Create a DBLine3D with the value set to the {@link LineString} provided.
	 *
	 * <p>
	 * This is a convenient way to assign a constant value in an expression or
	 * DBRow subclass.
	 *
	 * @param lineString
	 */
	public DBLine3D(LineString lineString) {
		super(lineString);
	}

	/**
	 * Create a DBLine3D with the value set to the {@link MultiPoint2DResult multipoint value or expression} provided.
	 *
	 * <p>
	 * This is a convenient way to assign a constant value in an expression or
	 * DBRow subclass.
	 *
	 * @param multipoint either a {@link MultiPoint3DExpression} or a {@link DBMultiPoint3D}
	 */
	public DBLine3D(MultiPoint3DResult multipoint) {
		super(new MultiPoint3DExpression(multipoint).line3DResult());
	}

	/**
	 * Create a DBLine3D using the expression supplied.
	 *
	 * <p>
	 * Useful for defining expression columns in DBRow subclass that acquire their
	 * value from a transformation of data at query time.
	 *
	 * @param columnExpression
	 */
	public DBLine3D(Line3DExpression columnExpression) {
		super(columnExpression);
	}

	/**
	 * Set the value of this DBLine3D to the value provided.
	 *
	 * <p>
	 * Use this method to define the value of a field/column before inserting the
	 * DBRow subclass into the database.
	 *
	 * @param line
	 */
	public void setValue(LineString line) {
		setLiteralValue(line);
	}

	/**
	 * Set the value of this DBLine3D to the value provided.
	 *
	 * <p>
	 * The series of points will combined into a line for you.
	 *
	 * <p>
	 * Use this method to define the value of a field/column before inserting the
	 * DBRow subclass into the database.
	 *
	 * @param points
	 */
	public void setValue(Point... points) {
		GeometryFactory geometryFactory = new GeometryFactory();
		List<Coordinate> coords = new ArrayList<>();
		for (Point point : points) {
			coords.add(point.getCoordinate());
		}
		LineString line = geometryFactory.createLineString(coords.toArray(new Coordinate[]{}));
		setLiteralValue(line);
	}

	/**
	 * Set the value of this DBLine3D to the value provided.
	 *
	 * <p>
	 * The series of coordinates will combined into a line for you.
	 *
	 * <p>
	 * Use this method to define the value of a field/column before inserting the
	 * DBRow subclass into the database.
	 *
	 * @param coords
	 */
	public void setValue(Coordinate... coords) {
		GeometryFactory geometryFactory = new GeometryFactory();
		LineString line = geometryFactory.createLineString(coords);
		setLiteralValue(line);
	}

	@Override
	public LineString getValue() {
		if (!isDefined() || isNull()) {
			return null;
		} else {
			return getLiteralValue();
		}
	}

	/**
	 * Transform the value of the DBLine3D into a
	 * {@link LineString JTS LineString}
	 *
	 * @return the value of this object if defined and not NULL, NULL otherwise.
	 */
	public LineString jtsLineStringValue() {
		return getValue();
	}

	@Override
	public String getSQLDatatype() {
		return " LINESTRING ";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		LineString lineString = getValue();
		if (lineString == null) {
			return db.getDefinition().getNull();
		} else {
			String str = db.getDefinition().transformLineStringIntoDatabaseLine2DFormat(lineString);
			return str;
		}
	}

	@Override
	protected LineString getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException, IncorrectGeometryReturnedForDatatype {

		LineString lineString = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				lineString = database.getDefinition().transformDatabaseLine2DValueToJTSLineString(string);
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(DBPoint3D.class.getName()).log(Level.SEVERE, null, ex);
				throw new ParsingSpatialValueException(fullColumnName, string,ex);
			}
			return lineString;
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
		return Line3DExpression.value(this).stringResult();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
}
