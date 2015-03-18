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
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.Geometry2DResult;

public class DBGeometry2D extends QueryableDatatype implements TransformRequiredForSelectClause, Geometry2DResult {

	private static final long serialVersionUID = 1L;

	public DBGeometry2D() {
	}

	public void setValue(Geometry geometry) {
		setLiteralValue(geometry);
	}

	public DBGeometry2D(nz.co.gregs.dbvolution.expressions.Geometry2DExpression columnExpression) {
		super(columnExpression);
	}

	public DBGeometry2D(Geometry geometry) {
		super(geometry);
	}

	@Override
	public String getSQLDatatype() {
		return "GEOMETRY";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		Geometry geom = (Geometry) getLiteralValue();
		String wktValue = geom.toText();
		return "GeomFromText('" + wktValue + "')";
	}

	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {

		Geometry geometry = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				geometry = database.getDefinition().transformDatabaseValueToJTSGeometry(string);
			} catch (ParseException ex) {
				Logger.getLogger(DBGeometry2D.class.getName()).log(Level.SEVERE, null, ex);
				throw new nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException(fullColumnName, string);
			}
			return geometry;
		}
	}

	public Geometry getGeometryValue() {
		return (Geometry) ((this.getLiteralValue() != null) ? this.getLiteralValue() : null);
	}

	@Override
	public Geometry getValue() {
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
