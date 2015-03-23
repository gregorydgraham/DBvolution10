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

import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import com.vividsolutions.jts.geom.LineString;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.exceptions.IncorrectGeometryReturnedForDatatype;
import nz.co.gregs.dbvolution.exceptions.ParsingSpatialValueException;
import nz.co.gregs.dbvolution.expressions.Line2DResult;

public class DBLine2D extends QueryableDatatype implements Line2DResult {

	private static final long serialVersionUID = 1L;

	public DBLine2D() {
	}

	public DBLine2D(LineString lineCollection) {
		super(lineCollection);
	}

	public DBLine2D(nz.co.gregs.dbvolution.expressions.Line2DExpression columnExpression) {
		super(columnExpression);
	}

	public void setValue(LineString line) {
		setLiteralValue(line);
	}

	@Override
	public LineString getValue() {
		if (!isDefined() || isNull()) {
			return null;
		} else {
			return (LineString) getLiteralValue();
		}
	}
	
	public LineString jtsLineStringValue(){
		return getValue();
	}

	@Override
	public String getSQLDatatype() {
		return "LINESTRING";
	}

	@Override
	protected String formatValueForSQLStatement(DBDatabase db) {
		LineString lineString = getValue();
		if (lineString == null) {
			return db.getDefinition().getNull();
		} else {
			String str = db.getDefinition().transformLineStringIntoDatabaseFormat(lineString);
			return str;
		}
	}

	@Override
	protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException, IncorrectGeometryReturnedForDatatype {

		LineString lineString = null;
		String string = resultSet.getString(fullColumnName);
		if (string == null) {
			return null;
		} else {
			try {
				lineString = database.getDefinition().transformDatabaseValueToJTSLineString(string);
			} catch (com.vividsolutions.jts.io.ParseException ex) {
				Logger.getLogger(DBPoint2D.class.getName()).log(Level.SEVERE, null, ex);
				throw new ParsingSpatialValueException(fullColumnName, string);
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
	
}
