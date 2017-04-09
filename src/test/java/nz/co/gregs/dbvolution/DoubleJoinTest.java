/*
 * Copyright 2013 Gregory Graham.
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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.*;
import static org.hamcrest.Matchers.is;

public class DoubleJoinTest extends AbstractTest {

	public DoubleJoinTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void fake() throws SQLException {
	}

	@Test
	public void doubleJoinTest() throws SQLException {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new DoubleJoinTest.DoubleLinkedWithSubclasses());
		database.createTable(new DoubleJoinTest.DoubleLinkedWithSubclasses());
		final DoubleLinkedWithSubclasses doubleLinked = new DoubleJoinTest.DoubleLinkedWithSubclasses();
		doubleLinked.uidDoubleLink.setValue(1);
		doubleLinked.manufacturer.setValue(1);
		doubleLinked.marketer.setValue(4);
		database.insert(doubleLinked);
		final DoubleLinkedWithSubclasses doubleLinked1 = new DoubleJoinTest.DoubleLinkedWithSubclasses();
		final Manufacturer manufacturer = new DoubleJoinTest.Manufacturer();
		final Marketer marketer = new DoubleJoinTest.Marketer();
		DBQuery query = database.getDBQuery(doubleLinked1, manufacturer, marketer);
		query.setBlankQueryAllowed(true);

		List<DBQueryRow> allRows = query.getAllRows();
		Assert.assertThat(allRows.size(),
				is(1));
		Assert.assertThat(allRows.get(0).get(marketer).uidCarCompany.getValue().intValue(),
				is(doubleLinked.marketer.getValue().intValue()));
		Assert.assertThat(allRows.get(0).get(manufacturer).uidCarCompany.getValue().intValue(),
				is(doubleLinked.manufacturer.getValue().intValue())
		);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new DoubleJoinTest.DoubleLinkedWithSubclasses());
	}

	@Test
	public void doubleJoinWithSameClassTest() throws SQLException {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new DoubleJoinTest.DoubleLinkedWithClass());
		database.createTable(new DoubleJoinTest.DoubleLinkedWithClass());
		final DoubleLinkedWithClass doubleLinked = new DoubleJoinTest.DoubleLinkedWithClass();
		doubleLinked.uidDoubleLink.setValue(1);
		doubleLinked.manufacturer.setValue(1);
		doubleLinked.marketer.setValue(4);
		database.insert(doubleLinked);
		final DoubleLinkedWithClass doubleLinked1 = new DoubleJoinTest.DoubleLinkedWithClass();
		final CarCompany carco = new CarCompany();
		DBQuery query = database.getDBQuery(doubleLinked1, carco);
		query.setBlankQueryAllowed(true);

		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(
				allRows.size(),
				is(0));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new DoubleJoinTest.DoubleLinkedWithClass());
	}

	@Test
	public void doubleJoinWithSameClassAndIDTest() throws SQLException {
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new DoubleJoinTest.DoubleLinkedWithClass());
		database.createTable(new DoubleJoinTest.DoubleLinkedWithClass());
		final DoubleLinkedWithClass doubleLinked = new DoubleJoinTest.DoubleLinkedWithClass();
		doubleLinked.uidDoubleLink.setValue(1);
		doubleLinked.manufacturer.setValue(1);
		doubleLinked.marketer.setValue(1);
		database.insert(doubleLinked);
		final DoubleLinkedWithClass doubleLinked1 = new DoubleJoinTest.DoubleLinkedWithClass();
		final CarCompany carco = new CarCompany();
		DBQuery query = database.getDBQuery(doubleLinked1, carco);
		query.setBlankQueryAllowed(true);

		List<DBQueryRow> allRows = query.getAllRows();

		Assert.assertThat(
				allRows.size(),
				is(1));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new DoubleJoinTest.DoubleLinkedWithClass());
	}

	@DBTableName("double_linked_with_subclasses")
	public static class DoubleLinkedWithSubclasses extends DBRow {

		public static final long serialVersionUID = 1L;
		@DBPrimaryKey()
		@DBColumn("uid_doublellink")
		DBInteger uidDoubleLink = new DBInteger();

		@DBForeignKey(Manufacturer.class)
		@DBColumn("fkmanufacturer")
		DBInteger manufacturer = new DBInteger();

		@DBForeignKey(Marketer.class)
		@DBColumn("fkmarketer")
		DBInteger marketer = new DBInteger();

	}

	@DBTableName("double_linked_carco")
	public static class DoubleLinkedWithClass extends DBRow {

		public static final long serialVersionUID = 1L;
		@DBPrimaryKey()
		@DBColumn("uid_doublellink")
		DBInteger uidDoubleLink = new DBInteger();

		@DBForeignKey(CarCompany.class)
		@DBColumn("fkmanufacturer")
		DBInteger manufacturer = new DBInteger();

		@DBForeignKey(CarCompany.class)
		@DBColumn("fkmarketer")
		DBInteger marketer = new DBInteger();

	}

	public static class Manufacturer extends CarCompany {

		public static final long serialVersionUID = 1L;
	}

	public static class Marketer extends CarCompany {

		public static final long serialVersionUID = 1L;
	}
}
