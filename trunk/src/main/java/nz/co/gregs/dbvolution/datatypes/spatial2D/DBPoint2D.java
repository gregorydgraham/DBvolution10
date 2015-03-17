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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException;
import nz.co.gregs.dbvolution.expressions.Point2DResult;

public class DBPoint2D extends QueryableDatatype implements Point2DResult {

	private static final long serialVersionUID = 1L;

	public DBPoint2D() {
	}

	public void setValue(Point point) {
		setLiteralValue(point);
	}

	public DBPoint2D(nz.co.gregs.dbvolution.expressions.Point2DExpression columnExpression) {
		super(columnExpression);
	}

	public DBPoint2D(Point point) {
		super(point);
	}

	@Override
	public String getSQLDatatype() {
		return " POINT ";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		Point point = (Point) getLiteralValue();
		String wktValue = point.toText();
		return "PointFromText('" + wktValue + "')";
	}

	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException, IncorrectGeometryReturnedForDatatype {

		Point point = null;
		WKTReader wktReader = new WKTReader();
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				Geometry geometry = wktReader.read(string);
				if (geometry instanceof Point) {
					point = (Point) geometry;
				} else {
					throw new IncorrectGeometryReturnedForDatatype(geometry, point);
				}
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
