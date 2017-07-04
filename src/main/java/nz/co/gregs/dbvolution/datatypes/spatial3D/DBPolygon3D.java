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

import com.vividsolutions.jts.geom.Polygon;
import nz.co.gregs.dbvolution.datatypes.TransformRequiredForSelectClause;
import com.vividsolutions.jts.io.ParseException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.Polygon3DExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.Polygon3DResult;

/**
 * Represents database columns and values that are a 2 dimensional polygon: an
 * closed ordered set of X and Y values defining a solid shape.
 *
 * <p>
 * Use DBPolygon3D when the column is a 2 dimensional {@code Polygon},
 * {@code ST_Polygon}, or {@code GEOMETRY} that represents a polygon.
 *
 * <p>
 * Generally DBPolygon3D is declared inside your DBRow sub-class as:
 * {@code @DBColumn public DBPolygon3D myPolygonColumn = new DBPolygon3D();}
 *
 *
 * @author Gregory Graham
 */
public class DBPolygon3D extends QueryableDatatype<PolygonZ> implements TransformRequiredForSelectClause, Polygon3DResult {

	private static final long serialVersionUID = 1L;

	/**
	 * Create an unset undefined DBPolygon3D object to represent a Polygon column
	 * or value.
	 *
	 */
	public DBPolygon3D() {
	}

	/**
	 * Set the value of this DBPolygon3D to the {@link Polygon} specified.
	 *
	 * <p>
	 * Set values are used to add the value to the database. Without a set value
	 * the database entry will be NULL.
	 *
	 * @param polygon the value to be set in the database.
	 */
	public void setValue(PolygonZ polygon) {
		setLiteralValue(polygon);
	}

	/**
	 * Create a DBPolygon3D with the column expression specified.
	 *
	 * <p>
	 * When retrieving this object from the database the expression will be
	 * evaluated to provide the value.
	 *
	 * @param columnExpression
	 */
	public DBPolygon3D(nz.co.gregs.dbvolution.expressions.Polygon3DExpression columnExpression) {
		super(columnExpression);
	}

	/**
	 * Create DBPolygon3D and set it's value to the JTS {@link  Polygon} provided.
	 *
	 * <p>
	 * Equivalent to {code polygon3D = new DBPolygon3D();
	 * polygon3D.setValue(aPolygon);}
	 *
	 * @param polygon
	 */
	public DBPolygon3D(PolygonZ polygon) {
		super(polygon);
	}

	@Override
	public String getSQLDatatype() {
		return "POLYGONZ";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		PolygonZ geom = getLiteralValue();
		return db.getDefinition().transformPolygonIntoDatabasePolygon3DFormat(geom);
	}

	@Override
	protected PolygonZ getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {

		PolygonZ geometry = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				geometry =database.getDefinition().transformDatabasePolygon3DToPolygonZ(string);
			} catch (ParseException ex) {
				Logger.getLogger(DBPolygon3D.class.getName()).log(Level.SEVERE, null, ex);
				throw new nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException(fullColumnName, string, ex);
			}
			return geometry;
		}
	}

	/**
	 * Convert the value of this object to a {@link PolygonZ}.
	 *
	 * <p>
	 * NULL is valid result from this method.
	 *
	 * @return the set value of this object as a PolygonZ object.
	 */
	public PolygonZ polygonZValue() {
		return (this.getLiteralValue() != null) ? this.getLiteralValue() : null;
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
		return polygonZValue();
	}

	@Override
	public PolygonZ getValue() {
		return polygonZValue();
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
		return Polygon3DExpression.value(this).stringResult();
	}

	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
}
