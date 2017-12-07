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
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
		Assert.assertThat(
				testableSQL(sqlForQuery),
				anyOf(
						is(testableSQLWithoutColumnAliases("select __78874071.name db_241667647, __78874071.uid_carcompany db112832814, _1617907935.fk_car_company db_238514883, _1617907935.fk_company_logo db_1915875486 from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name db_242652735, __78874071.uid_carcompany db559213902, _1617907935.fk_car_company db1646770429, _1617907935.fk_company_logo db_1568052350 from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) ;")),
						is(testableSQLWithoutColumnAliases("select 78874071.name db1834098234, 78874071.uid_carcompany db_144385433, 1617907935.fk_car_company db_1191378222, 1617907935.fk_company_logo db_1389867923 from car_company 78874071 inner join lt_carco_logo 1617907935 on( 1617907935.fk_car_company = 78874071.uid_carcompany )"))
				));
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
						is(testableSQLWithoutColumnAliases("select __78874071.name db_241667647, __78874071.uid_carcompany db112832814, _1617907935.fk_car_company db_238514883, _1617907935.fk_company_logo db_1915875486, _1159239592.logo_id db_1579317226, _1159239592.car_company_fk db1430605643, _1159239592.image_file db1622411417, _1159239592.image_name db1622642088 from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) inner join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name db_241667647, __78874071.uid_carcompany db112832814, _1159239592.logo_id db_1579317226, _1159239592.car_company_fk db1430605643, _1159239592.image_file db1622411417, _1159239592.image_name db1622642088, _1617907935.fk_car_company db_238514883, _1617907935.fk_company_logo db_1915875486 from car_company as __78874071 inner join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name db_242652735, __78874071.uid_carcompany db559213902, _1159239592.logo_id db_861303786, _1159239592.car_company_fk db1877017483, _1159239592.image_file db2137056441, _1159239592.image_name db2137287112, _1617907935.fk_car_company db1646770429, _1617907935.fk_company_logo db_1568052350 from car_company as __78874071 inner join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name db_242652735, __78874071.uid_carcompany db559213902, _1617907935.fk_car_company db1646770429, _1617907935.fk_company_logo db_1568052350, _1159239592.logo_id db_861303786, _1159239592.car_company_fk db1877017483, _1159239592.image_file db2137056441, _1159239592.image_name db2137287112 from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) inner join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQLWithoutColumnAliases("select 78874071.name db1834098234, 78874071.uid_carcompany db_144385433, 1159239592.logo_id db_1946404919, 1159239592.car_company_fk db_41541576, 1159239592.image_file db699684870, 1159239592.image_name db699915541, 1617907935.fk_car_company db_1191378222, 1617907935.fk_company_logo db_1389867923 from car_company 78874071 inner join companylogo 1159239592 on( 1159239592.car_company_fk = 78874071.uid_carcompany ) inner join lt_carco_logo 1617907935 on( 1617907935.fk_car_company = 78874071.uid_carcompany and 1617907935.fk_company_logo = 1159239592.logo_id )")),
						is(testableSQLWithoutColumnAliases("select 78874071.name db1834098234, 78874071.uid_carcompany db_144385433, 1617907935.fk_car_company db_1191378222, 1617907935.fk_company_logo db_1389867923, 1159239592.logo_id db_1946404919, 1159239592.car_company_fk db_41541576, 1159239592.image_file db699684870, 1159239592.image_name db699915541 from car_company 78874071 inner join lt_carco_logo 1617907935 on( 1617907935.fk_car_company = 78874071.uid_carcompany ) inner join companylogo 1159239592 on( 1159239592.car_company_fk = 78874071.uid_carcompany and 1617907935.fk_company_logo = 1159239592.logo_id )"))
				)
		);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(0));
	}

	@Test(expected = AccidentalCartesianJoinException.class)
	public void testCartesianJoinProtection() throws SQLException, Exception {
		DBQuery dbQuery = database.getDBQuery(new Marque(), new CompanyLogo());
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
	}
}
