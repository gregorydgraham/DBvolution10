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
package nz.co.gregs.dbvolution.datatypes;

import com.vividsolutions.jts.geom.*;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.SpatialDatabase;
import nz.co.gregs.dbvolution.datatypes.spatial.DBGeometry;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

public class SpatialTest extends AbstractTest {

	public SpatialTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void basicSpatialTest() throws SQLException {
		if (database instanceof SpatialDatabase) {
			BasicSpatialTable spatial = new BasicSpatialTable();
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(spatial);
			database.createTable(spatial);

			GeometryFactory fac = new GeometryFactory();
			Point createPoint = fac.createPoint(new Coordinate(5, 10));
			spatial.myfirstgeom.setValue(createPoint);
			database.insert(spatial);

			List<BasicSpatialTable> allRows = database.getDBTable(new BasicSpatialTable()).setBlankQueryAllowed(true).getAllRows();
			Assert.assertThat(allRows.size(), is(1));

			Assert.assertThat(allRows.get(0).myfirstgeom.getValue().getGeometryType(), is("Point"));
		}
	}

	public static class BasicSpatialTable extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkid = new DBInteger();

		@DBColumn
		DBGeometry myfirstgeom = new DBGeometry();

	}

}
