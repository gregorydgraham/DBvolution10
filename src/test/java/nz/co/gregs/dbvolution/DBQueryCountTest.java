/*
 * Copyright 2014 Gregory Graham.
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
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBQueryCountTest extends AbstractTest {

	public DBQueryCountTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void simpleCountTest() throws SQLException {

		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		dbQuery.add(new Marque());
		dbQuery.add(carCompany);
//		final String generateSQLString = dbQuery.getSQLForCount();

//		String expectedResultUsingONClause = "select count(*) from marque as __1997432637 inner join car_company as __78874071 on( (( __78874071.name) = 'toyota')) and (__1997432637.fk_carcompany = __78874071.uid_carcompany) ) ;";
//		String expectedResultUsingWHEREClause   = "select count(*) from marque as __1997432637 inner join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) where 1=1 and (( __78874071.name) = 'toyota') ;";
//		String expectedResultUsingWHEREClause2 = "select count(*) from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) where 1=1 and (( __78874071.name) = 'toyota') ;";
//		String expectedResultUsingWHEREClauseFromOracle = "select count(*) from car_company __78874071 inner join marque __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) where 1=1 and (( __78874071.name) = 'toyota')";
//		Assert.assertThat(testableSQLWithoutColumnAliases(generateSQLString),
//				anyOf(is(testableSQLWithoutColumnAliases(expectedResultUsingONClause)),
//						is(testableSQLWithoutColumnAliases(expectedResultUsingWHEREClause)),
//						is(testableSQLWithoutColumnAliases(expectedResultUsingWHEREClause2)),
//						is(testableSQLWithoutColumnAliases(expectedResultUsingWHEREClauseFromOracle))
//				));
		// make sure it works
		Long count = dbQuery.count();
		Assert.assertThat(count, is(2L));

	}

	@Test
	public void countOtherMarquesTests() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("OTHER");
		dbQuery.add(new Marque());
		dbQuery.add(carCompany);
		// make sure it works
		Long count = dbQuery.count();
		Assert.assertThat(count, is(16L));

		carCompany.name.permittedValues("Ford");
		dbQuery.add(new Marque());
		dbQuery.add(carCompany);
		// make sure it works
		count = dbQuery.count();
		Assert.assertThat(count, is(1L));

		carCompany.name.permittedValues("GENERAL MOTORS");
		dbQuery.add(new Marque());
		dbQuery.add(carCompany);
		// make sure it works
		count = dbQuery.count();
		Assert.assertThat(count, is(3L));

	}

	@Test
	public void countSingleTableTests() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("OTHER");
		dbQuery.add(carCompany);
		// make sure it works
		Long count = dbQuery.count();
		Assert.assertThat(count, is(1L));

		carCompany.name.permittedPattern("%O%");
		dbQuery.add(carCompany);
		// make sure it works
		count = dbQuery.count();

		Assert.assertThat(count, is(3L));

		carCompany.name.permittedRange("F", "O");
		dbQuery.add(carCompany);
		// make sure it works
		count = dbQuery.count();
		Assert.assertThat(count, is(2L));

	}

	@Test
	public void countOuterJoinTests() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("OTHER");
		dbQuery.addOptional(carCompany);
		dbQuery.add(new Marque());
		// make sure it works
		Long count = dbQuery.count();
		Assert.assertThat(count, is(22L));

		dbQuery.addOptional(new CompanyLogo());
		count = dbQuery.count();
		Assert.assertThat(count, is(22L));
	}

}
