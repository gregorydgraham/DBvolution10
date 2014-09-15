/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.databases;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import static org.hamcrest.Matchers.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class Oracle11DBTest {

	private static Oracle11DB database = new Oracle11DB("localhost", 1521, "XE", "dbv", "dbv");

	public Oracle11DBTest() {
	}

	@BeforeClass
	public static void setUp() throws SQLException {
		database.preventDroppingOfTables(false);
		DBVTestTable dbvTestTable = new DBVTestTable();
		database.dropTableNoExceptions(dbvTestTable);
		database.preventDroppingOfTables(true);

		database.createTable(dbvTestTable);

		dbvTestTable.name.setValue("First");
		database.insert(dbvTestTable);
		dbvTestTable = new DBVTestTable();
		dbvTestTable.name.setValue("2nd");
		database.insert(dbvTestTable);
	}

	@AfterClass
	public static void tearDown() {
		database.preventDroppingOfTables(false);
		DBVTestTable dbvTestTable = new DBVTestTable();
		database.dropTableNoExceptions(dbvTestTable);
		database.preventDroppingOfTables(true);
	}
	
	@Test 
	public void simpleTest() throws SQLException{
		List<DBVTestTable> allRows = database.getDBTable(new DBVTestTable()).setBlankQueryAllowed(true).getAllRows();
		Assert.assertThat(allRows.size(), is(2));
	}

	@DBTableName("dbvt11t")
	public static class DBVTestTable extends DBRow {

		private static final long serialVersionUID = 1L;

		public DBVTestTable() {
			super();
		}

		@DBColumn("pkcoltest")
		@DBPrimaryKey
		@DBAutoIncrement
		DBInteger pkColumnForTest = new DBInteger();

		@DBColumn
		DBString name = new DBString();

	}

}
