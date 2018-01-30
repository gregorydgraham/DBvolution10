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
package nz.co.gregs.dbvolution.datatypes.spatial2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.columns.Line2DColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException;
import nz.co.gregs.dbvolution.expressions.spatial2D.Line2DExpression;
import nz.co.gregs.dbvolution.expressions.spatial2D.MultiPoint2DExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.Line2DResult;
import nz.co.gregs.dbvolution.results.MultiPoint2DResult;

/**
 * Represents datatypes and columns that are composed of a series of points
 * connected as a line.
 *
 * <p>
 * Use this type if the database column stores a series of 2-dimensional (that
 * is X and Y points) that are contiguous and open.
 *
 * <p>
 * Alternatives to a DBLine2D are single line segments {@link DBLineSegment2D},
 * infinite lines (#TODO), closed paths (#TODO), and closed paths defining a
 * solid {@link DBPolygon2D}.
 *
 * <p>
 * Common datatypes covered by this type include LINESTRING.
 *
 * <p>
 * Spatial types are not automatically generated during schema extraction so you
 * may need to change some DBString fields.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBLine2D extends QueryableDatatype<LineString> implements Line2DResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor.
	 *
	 * Use this method to create the DBLine2D used in your DBRow subclass.
	 *
	 */
	public DBLine2D() {
	}

	/**
	 * Create a DBLine2D with the value set to the {@link LineString} provided.
	 *
	 * <p>
 This is a convenient way to assign a constant value in an value or
 DBRow subclass.
	 *
	 * @param lineString
	 */
	public DBLine2D(LineString lineString) {
		super(lineString);
	}

	/**
	 * Create a DBLine2D with the value set to the {@link LineString} provided.
	 *
	 * <p>
 This is a convenient way to assign a constant value in an value or
 DBRow subclass.
	 *
	 * @param lineString
	 */
	public DBLine2D(Point... lineString) {
		super(lineStringFromPoints(lineString));
	}
	
	private static LineString lineStringFromPoints(Point... points){
		GeometryFactory geometryFactory = new GeometryFactory();
		List<Coordinate> coords = new ArrayList<Coordinate>();
		for (Point point : points) {
			coords.add(point.getCoordinate());
		}
		LineString line = geometryFactory.createLineString(coords.toArray(new Coordinate[]{}));
		return line;
	}

	/**
	 * Create a DBLine2D with the value set to the
	 * {@link MultiPoint2DResult multipoint value or value} provided.
	 *
	 * <p>
 This is a convenient way to assign a constant value in an value or
 DBRow subclass.
	 *
	 * @param multipoint either a {@link MultiPoint2DExpression} or a
	 * {@link DBMultiPoint2D}
	 */
	public DBLine2D(MultiPoint2DResult multipoint) {
		super(new MultiPoint2DExpression(multipoint).line2DResult());
	}

	/**
	 * Create a DBLine2D using the value supplied.
	 *
	 * <p>
 Useful for defining value columns in DBRow subclass that acquire their
 value from a transformation of data at query time.
	 *
	 * @param columnExpression
	 */
	public DBLine2D(nz.co.gregs.dbvolution.expressions.spatial2D.Line2DExpression columnExpression) {
		super(columnExpression);
	}

	public DBLine2D(Coordinate... coords) {
		super(linestringFromCoords(coords));
	}
	
	private static LineString linestringFromCoords(Coordinate... coords){
		GeometryFactory geometryFactory = new GeometryFactory();
		LineString line = geometryFactory.createLineString(coords);
		return line;
	}

	/**
	 * Set the value of this DBLine2D to the value provided.
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
	 * Set the value of this DBLine2D to the value provided.
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
	 * Set the value of this DBLine2D to the value provided.
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
	 * Transform the value of the DBLine2D into a
	 * {@link LineString JTS LineString}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	protected String formatValueForSQLStatement(DBDefinition db) {
		LineString lineString = getValue();
		if (lineString == null) {
			return db.getNull();
		} else {
			String str = db.transformLineStringIntoDatabaseLine2DFormat(lineString);
			return str;
		}
	}

	@Override
	protected LineString getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException, IncorrectGeometryReturnedForDatatype {

		LineString lineString = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				lineString = database.transformDatabaseLine2DValueToJTSLineString(string);
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(DBPoint2D.class.getName()).log(Level.SEVERE, null, ex);
				throw new ParsingSpatialValueException(fullColumnName, string, ex);
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
		return Line2DExpression.value(this).stringResult();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Line2DColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new Line2DColumn(row, this);
	}
}
