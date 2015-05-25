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

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException;
import nz.co.gregs.dbvolution.expressions.Point2DResult;

/**
 * Represents database columns and values that are a 2 dimensional point: an
 * pair of X and Y values.
 *
 * <p>
 * Use DBPoint2D when the column is a 2 dimensional {@code Point},
 * {@code ST_Point}, or {@code GEOMETRY} that represents a point.
 *
 * <p>
 * Generally DBPoint2D is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBPoint2D myPointColumn = new DBPoint2D();}
 *
 *
 * @author Gregory Graham
 */
public class DBPoint2D extends QueryableDatatype implements Point2DResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Create an unset undefined DBPoint2D object to represent a Point column or
	 * value.
	 *
	 */
	public DBPoint2D() {
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
	public void setValue(Point point) {
		setLiteralValue(point);
	}

	@Override
	public Point getValue() {
		if (!isDefined() || isNull()) {
			return null;
		} else {
			return (Point) getLiteralValue();
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
	public Point jtsPointValue() {
		return getValue();
	}

	/**
	 * Create a DBPoint2D with the column expression specified.
	 *
	 * <p>
	 * When retrieving this object from the database the expression will be
	 * evaluated to provide the value.
	 *
	 * @param columnExpression
	 */
	public DBPoint2D(nz.co.gregs.dbvolution.expressions.Point2DExpression columnExpression) {
		super(columnExpression);
	}

	/**
	 * Create DBpoint2D and set it's value to the JTS {@link  Point} provided.
	 *
	 * <p>
	 * Equivalent to {code point2D = new DBPoint2D(); point2D.setValue(aPoint);}
	 *
	 * @param point
	 */
	public DBPoint2D(Point point) {
		super(point);
	}

	@Override
	public String getSQLDatatype() {
		return " POINT ";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		Point point = getValue();
		if (point == null) {
			return db.getDefinition().getNull();
		} else {
			String str = db.getDefinition().transformPoint2DIntoDatabaseFormat(point);
			return str;
		}
	}

	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException, IncorrectGeometryReturnedForDatatype {

		Point point = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				point = database.getDefinition().transformDatabasePoint2DValueToJTSPoint(string);
			} catch (ParseException ex) {
				Logger.getLogger(DBPoint2D.class.getName()).log(Level.SEVERE, null, ex);
				throw new ParsingSpatialValueException(fullColumnName, string);
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

}
