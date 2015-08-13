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

import nz.co.gregs.dbvolution.datatypes.TransformRequiredForSelectClause;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.Polygon2DResult;

/**
 * Represents database columns and values that are a 2 dimensional polygon: an
 * closed ordered set of X and Y values defining a solid shape.
 *
 * <p>
 * Use DBPolygon2D when the column is a 2 dimensional {@code Polygon},
 * {@code ST_Polygon}, or {@code GEOMETRY} that represents a polygon.
 *
 * <p>
 * Generally DBPolygon2D is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBPolygon2D myPolygonColumn = new DBPolygon2D();}
 *
 *
 * @author Gregory Graham
 */
public class DBPolygon2D extends QueryableDatatype implements TransformRequiredForSelectClause, Polygon2DResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Create an unset undefined DBPolygon2D object to represent a Polygon column
	 * or value.
	 *
	 */
	public DBPolygon2D() {
	}

	/**
	 * Set the value of this DBPolygon2D to the {@link Polygon} specified.
	 *
	 * <p>
	 * Set values are used to add the value to the database. Without a set value
	 * the database entry will be NULL.
	 *
	 * @param polygon the value to be set in the database.
	 */
	public void setValue(Polygon polygon) {
		setLiteralValue(polygon);
	}

	/**
	 * Create a DBPolygon2D with the column expression specified.
	 *
	 * <p>
	 * When retrieving this object from the database the expression will be
	 * evaluated to provide the value.
	 *
	 * @param columnExpression
	 */
	public DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression columnExpression) {
		super(columnExpression);
	}

	/**
	 * Create DBPolygon2D and set it's value to the JTS {@link  Polygon} provided.
	 *
	 * <p>
	 * Equivalent to {code polygon2D = new DBPolygon2D();
	 * polygon2D.setValue(aPolygon);}
	 *
	 * @param polygon
	 */
	public DBPolygon2D(Polygon polygon) {
		super(polygon);
	}

	@Override
	public String getSQLDatatype() {
		return "POLYGON";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		Polygon geom = (Polygon) getLiteralValue();
		return db.getDefinition().doDBPolygon2DFormatTransform(geom);
	}

	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {

		Polygon geometry = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				geometry = database.getDefinition().transformDatabasePolygon2DToJTSPolygon(string);
			} catch (ParseException ex) {
				Logger.getLogger(DBPolygon2D.class.getName()).log(Level.SEVERE, null, ex);
				throw new nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException(fullColumnName, string, ex);
			}
			return geometry;
		}
	}

	/**
	 * Convert the value of this object to a JTS {@link Polygon}.
	 *
	 * <p>
	 * NULL is valid result from this method.
	 *
	 * @return the set value of this object as a JTS Polygon object.
	 */
	public Polygon jtsPolygonValue() {
		return (Polygon) ((this.getLiteralValue() != null) ? this.getLiteralValue() : null);
	}

	@Override
	public Polygon getValue() {
		return jtsPolygonValue();
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
	public NumberExpression measurableDimensions() {
		return NumberExpression.value(2);
	}

	@Override
	public NumberExpression spatialDimensions() {
		return NumberExpression.value(2);
	}

	@Override
	public BooleanExpression hasMagnitude() {
		return BooleanExpression.falseExpression();
	}

	@Override
	public NumberExpression magnitude() {
		return NumberExpression.value((Number)null);
	}
	
	@Override
	public StringExpression toWKTFormat(){
		return StringExpression.value(jtsPolygonValue().toText());
	}
}
