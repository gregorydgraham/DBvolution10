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
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import static org.hamcrest.Matchers.*;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Gregory Graham
 */
public class PrimaryKeylessTableTest extends AbstractTest {

	public PrimaryKeylessTableTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void linkIntoTableTest() throws SQLException {
		CarCompany carCompany = new CarCompany();
		LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();
		DBQuery dbQuery = database.getDBQuery(carCompany, link);
		dbQuery.setBlankQueryAllowed(true);
		String sqlForQuery = dbQuery.getSQLForQuery();
		Assert.assertThat(testableSQL(sqlForQuery), is(testableSQL("SELECT __78874071.name DB_241667647,\n"
				+ "__78874071.uid_carcompany DB112832814,\n"
				+ "_1617907935.fk_car_company DB_238514883,\n"
				+ "_1617907935.fk_company_logo DB_1915875486\n"
				+ " FROM  car_company AS __78874071 \n"
				+ " INNER JOIN lt_carco_logo AS _1617907935  ON( _1617907935.fk_car_company = __78874071.uid_carcompany ) \n"
				+ "\n"
				+ ";")));
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test
	public void linkThruTableTest() throws SQLException {
		CarCompany carCompany = new CarCompany();
		LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();

		DBQuery dbQuery = database.getDBQuery(carCompany, link, new CompanyLogo());
		dbQuery.setBlankQueryAllowed(true);
		String sqlForQuery = dbQuery.getSQLForQuery();
		Assert.assertThat(
				testableSQL(sqlForQuery), 
				anyOf(
						is(testableSQL("select __78874071.name db_241667647, __78874071.uid_carcompany db112832814, _1617907935.fk_car_company db_238514883, _1617907935.fk_company_logo db_1915875486, _1159239592.logo_id db_1579317226, _1159239592.car_company_fk db1430605643, _1159239592.image_file db1622411417, _1159239592.image_name db1622642088 from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) inner join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQL("select __78874071.name db_241667647, __78874071.uid_carcompany db112832814, _1159239592.logo_id db_1579317226, _1159239592.car_company_fk db1430605643, _1159239592.image_file db1622411417, _1159239592.image_name db1622642088, _1617907935.fk_car_company db_238514883, _1617907935.fk_company_logo db_1915875486 from car_company as __78874071 inner join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;"))
				)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test(expected = AccidentalCartesianJoinException.class)
	public void testCartesianJoinProtection() throws SQLException, Exception {
		DBQuery dbQuery = database.getDBQuery(new Marque(), new CompanyLogo());
		dbQuery.setBlankQueryAllowed(true);
		dbQuery.print();
	}
}
