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
import nz.co.gregs.dbvolution.expressions.Polygon2DResult;

public class DBPolygon2D extends QueryableDatatype implements TransformRequiredForSelectClause, Polygon2DResult {

	private static final long serialVersionUID = 1L;

	public DBPolygon2D() {
	}

	public void setValue(Polygon geometry) {
		setLiteralValue(geometry);
	}

	public DBPolygon2D(nz.co.gregs.dbvolution.expressions.Polygon2DExpression columnExpression) {
		super(columnExpression);
	}

	public DBPolygon2D(Polygon geometry) {
		super(geometry);
	}

	@Override
	public String getSQLDatatype() {
		return "POLYGON";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		Polygon geom = (Polygon) getLiteralValue();
		String wktValue = geom.toText();
		return "PolyFromText('" + wktValue + "')";
	}

	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {

		Polygon geometry = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				geometry = database.getDefinition().transformDatabaseValueToJTSPolygon(string);
			} catch (ParseException ex) {
				Logger.getLogger(DBPolygon2D.class.getName()).log(Level.SEVERE, null, ex);
				throw new nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException(fullColumnName, string);
			}
			return geometry;
		}
	}

	public Polygon getGeometryValue() {
		return (Polygon) ((this.getLiteralValue() != null) ? this.getLiteralValue() : null);
	}

	@Override
	public Polygon getValue() {
		return getGeometryValue();
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
