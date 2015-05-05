/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.internal.h2;

import java.sql.SQLException;
import java.sql.Statement;

//	static String ACTUAL_DATATYPE = "VARCHAR(2000)";

//	public static String DATATYPE = "DBV_POINT2D";
public enum Point2D {
	POINT2D("DBV_POINT2D", "VARCHAR(2000)", Point2DFunctions.values());
	private String datatype;
	private String actualType;
	private Point2DFunctions[] functions;

	Point2D(String datatype, String actualType, Point2DFunctions[] functions) {
		this.datatype = datatype;
		this.actualType = actualType;
		this.functions = functions;
	}

	@Override
	public String toString() {
		return datatype;
	}

	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP DOMAIN " + datatype + "; ");
		} catch (SQLException sqlex) {
			; // I don't care.
		}
		stmt.execute("CREATE DOMAIN IF NOT EXISTS " + datatype + " AS " + actualType + "; ");
		
		for (Point2DFunctions function : functions) {
			function.add(stmt);
		}
	}

	public String datatype() {
		return datatype;
	}
	
}
