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
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OuterJoinTest extends AbstractTest {

	public OuterJoinTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

//	@Test
//	public void testANSIJoinClauseCreation() throws Exception {
//		String lineSep = System.getProperty("line.separator");
//		Marque mrq = new Marque();
//
//		CarCompany carCo = new CarCompany();
//		QueryOptions opts = new QueryOptions();
//
//		System.out.println("" + mrq.getRelationshipsAsSQL(database, carCo, opts));
//		System.out.println("" + carCo.getRelationshipsAsSQL(database, mrq, opts));
//
//		String expectedString = "__1997432637.fk_carcompany = __78874071.uid_carcompany";
//		Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo, opts)), is(testableSQL(expectedString)));
//		expectedString = "__78874071.UID_CARCOMPANY = __1997432637.FK_CARCOMPANY";
//		Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq, opts)), is(testableSQL(expectedString)));
//
//	}

	@Test
	public void testANSIInnerJoinQueryCreation() throws Exception {
		Marque mrq = new Marque();
		CarCompany carCo = new CarCompany();
		LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();
		CompanyLogo logo = new CompanyLogo();

		DBQuery dbQuery = database.getDBQuery(mrq);
		List<DBRow> tables = new ArrayList<DBRow>();
		StringBuilder ansiJoinClause = new StringBuilder();
		ansiJoinClause.append(dbQuery.getANSIJoinClause(database, new DBQuery.QueryState(dbQuery, database), carCo, tables));
		System.out.println("=============");
		System.out.println(ansiJoinClause);
		System.out.println("=============");
		String expectedCarCoJoin = "car_company as __78874071";
		String expectedCarCoJoinOracle = "car_company __78874071";
		Assert.assertThat(testableSQL(ansiJoinClause.toString()),
				anyOf(
						is(testableSQL(expectedCarCoJoin)),
						is(testableSQL(expectedCarCoJoinOracle))
				));

		tables.add(carCo);
		ansiJoinClause.append(dbQuery.getANSIJoinClause(database, new DBQuery.QueryState(dbQuery, database), mrq, tables));
		System.out.println("=============");
		System.out.println(ansiJoinClause);
		System.out.println("=============");
		String expectedMarqueJoin1 = "car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany )";
		String expectedMarqueJoin2 = "car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany )";
		Assert.assertThat(
				testableSQL(ansiJoinClause.toString()),
				anyOf(
						is(testableSQL(expectedMarqueJoin1)),
						is(testableSQL(expectedMarqueJoin2))
				));

		tables.add(mrq);
		ansiJoinClause.append(dbQuery.getANSIJoinClause(database, new DBQuery.QueryState(dbQuery, database), link, tables));
		System.out.println("=============");
		System.out.println(ansiJoinClause);
		System.out.println("=============");
		String expectedLinkJoin = "car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company )";
		Assert.assertThat(
				testableSQL(ansiJoinClause.toString()),
				anyOf(
						is(testableSQL(expectedLinkJoin)),
						is(testableSQL("car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany )"))
				));

		tables.add(link);
		ansiJoinClause.append(dbQuery.getANSIJoinClause(database, new DBQuery.QueryState(dbQuery, database), logo, tables));
		System.out.println("=============");
		System.out.println(ansiJoinClause);
		System.out.println("=============");
		String expectedLogoJoin = "car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) inner join companylogo as _1159239592 on( __78874071.uid_carcompany = _1159239592.car_company_fk and _1617907935.fk_company_logo = _1159239592.logo_id )";
		Assert.assertThat(
				testableSQL(ansiJoinClause.toString()),
				anyOf(
						is(testableSQL(expectedLogoJoin)),
						is(testableSQL("car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) inner join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id )"))
				)
		);
	}

	@Test
	public void testANSIQueryCreation() throws SQLException, Exception {
		DBQuery dbQuery = database.getDBQuery(new CarCompany(), new Marque());
		dbQuery.setUseANSISyntax(true);
		dbQuery.setBlankQueryAllowed(true);
		String sqlForQuery = dbQuery.getSQLForQuery();
		String expected2TableQuery1
				= "select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) ;";
		String expected2TableQuery2 = "select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) ;";
		System.out.println(sqlForQuery);
		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(is(testableSQLWithoutColumnAliases(expected2TableQuery1)), is(testableSQLWithoutColumnAliases(expected2TableQuery2))));
		Assert.assertThat(dbQuery.count(), is(22L));
		dbQuery.printAllDataColumns(System.out);
		LinkCarCompanyAndLogo linkCoAndLogo = new LinkCarCompanyAndLogo();

		dbQuery.add(linkCoAndLogo);
		sqlForQuery = dbQuery.getSQLForQuery();
		String expected3TableQuery
				= testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) ;");
		String otherQueryVersion = testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) ;");
		System.out.println(sqlForQuery);
		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(is(expected3TableQuery),
						is(otherQueryVersion),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) ;"))
				));
		Assert.assertThat(dbQuery.count(), is(0L));
		dbQuery.printAllDataColumns(System.out);

		dbQuery.remove(linkCoAndLogo);
		dbQuery.addOptional(linkCoAndLogo);
		sqlForQuery = dbQuery.getSQLForQuery();
		String expected1OptionalTableQuery
				= testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) left outer join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) ;");
		String expected1OptionalTableQuery2
				= testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 left outer join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) ;");
		String expected1OptionalTableQuery3
				= testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) ;");
		System.out.println(sqlForQuery);
		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(is(expected1OptionalTableQuery),
						is(expected1OptionalTableQuery2),
						is(expected1OptionalTableQuery3),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) ;"))
				));
		dbQuery.print(System.out);
		Assert.assertThat(dbQuery.count(), is(22L));

		dbQuery.addOptional(new CompanyLogo());
		sqlForQuery = dbQuery.getSQLForQuery();
		System.out.println(sqlForQuery);
		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) left outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from car_company __78874071 inner join marque __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) left outer join companylogo _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id )"))
				));
		dbQuery.print(System.out);
		Assert.assertThat(dbQuery.count(), is(22L));

	}

	@Test
	public void testANSIFullOuterQueryCreation() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		dbQuery.setUseANSISyntax(true);
		dbQuery.addOptional(new Marque());
		dbQuery.addOptional(new CarCompany());
		dbQuery.addOptional(new LinkCarCompanyAndLogo());
		dbQuery.addOptional(new CompanyLogo());
		dbQuery.setBlankQueryAllowed(true);
		String sqlForQuery = dbQuery.getSQLForQuery();
		System.out.println(sqlForQuery);
		String expectedFullOuterQuery = testableSQLWithoutColumnAliases("select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name, _1617907935.fk_car_company, _1617907935.fk_company_logo from marque as __1997432637 full outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) full outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) full outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;");
		String otherExpectedFullOuterQuery = testableSQLWithoutColumnAliases("select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from marque as __1997432637 full outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) full outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) full outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;");
		System.out.println(testableSQLWithoutColumnAliases(sqlForQuery));
		System.out.println(testableSQLWithoutColumnAliases(expectedFullOuterQuery));
		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(is(expectedFullOuterQuery), is(otherExpectedFullOuterQuery)));
		// FULL OUTER JOIN not supported by H2 or MySqldb
		if (database.supportsFullOuterJoin()) {
			dbQuery.print(System.out);
			Assert.assertThat(dbQuery.count(), is(22L));
		}
	}

	@Test
	public void testOuterJoinQueryAvoidsBadOrder() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		dbQuery.setUseANSISyntax(true);
		dbQuery.addOptional(new LinkCarCompanyAndLogo());
		dbQuery.addOptional(new CompanyLogo());
		dbQuery.addOptional(new Marque());
		dbQuery.addOptional(new CarCompany());
		dbQuery.setBlankQueryAllowed(true);
		String sqlForQuery = dbQuery.getSQLForQuery();
		System.out.println(sqlForQuery);
		System.out.println(testableSQLWithoutColumnAliases(sqlForQuery));
		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				not(containsString("( 1=1 )")));
		// FULL OUTER JOIN not supported by H2 or MySqldb
		if (database.supportsFullOuterJoin()) {
			dbQuery.print(System.out);
			Assert.assertThat(dbQuery.count(), is(22L));
		}
	}

	@Test
	public void testOuterJoinQueryCreatesEmptyRows() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();
		dbQuery.setUseANSISyntax(true);
		dbQuery.add(new CarCompany());
		dbQuery.addOptional(new LinkCarCompanyAndLogo());
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(4));
		List<LinkCarCompanyAndLogo> links = dbQuery.getAllInstancesOf(new LinkCarCompanyAndLogo());
		Assert.assertThat(links.size(), is(0));
		for (DBQueryRow queryrow : allRows) {
			Assert.assertThat(queryrow.get(new LinkCarCompanyAndLogo()), is(nullValue()));
		}
	}

	@Test
	public void testSimpleCriteriaInOnClause() throws SQLException {
		DBQuery dbquery = database.getDBQuery();
		final CarCompany carCompany = new CarCompany();
		carCompany.name.permittedRangeInclusive("ford", "TOYOTA");
		dbquery.add(carCompany);
		final Marque marque = new Marque();
		marque.enabled.permittedValues(true);
		dbquery.addOptional(marque);
		String sqlForQuery = dbquery.getSQLForQuery();

		final String marqueCondition = "__1997432637.ENABLED = 1";
		Assert.assertThat(sqlForQuery.indexOf(marqueCondition), is(sqlForQuery.lastIndexOf(marqueCondition)));
		List<DBQueryRow> allRows = dbquery.getAllRows();
		System.out.println(sqlForQuery);
		database.print(allRows);
		dbquery = database.getDBQuery();
		dbquery.add(marque);
		dbquery.addOptional(carCompany);
		sqlForQuery = dbquery.getSQLForQuery();
		System.out.println(sqlForQuery);
		Assert.assertThat(testableSQL(sqlForQuery),
				allOf(
						containsString(testableSQL("( __78874071.name) >= 'ford'")),
						containsString(testableSQL("( __78874071.name) <= 'toyota'")),
						containsString(testableSQL("__1997432637.enabled = 1"))
				));
		final String carCompanyCondition = testableSQL("__78874071.NAME) >= 'ford' and (__78874071.NAME) <= 'TOYOTA'");
		final String testableQuery = testableSQL(sqlForQuery);
		Assert.assertThat(testableQuery.indexOf(carCompanyCondition), is(testableQuery.lastIndexOf(carCompanyCondition)));

//		database.print(dbquery.getAllRows());
	}

	@Test
	public void testFullOuterJoinWithSimpleCriteria() throws SQLException {
		DBQuery dbquery = database.getDBQuery();

		final CarCompany carCompany = new CarCompany();
		final Marque marque = new Marque();

		carCompany.name.permittedRangeInclusive("ford", "TOYOTA");
		marque.enabled.permittedValues(Boolean.TRUE);

		dbquery.addOptional(marque);
		dbquery.addOptional(carCompany);

		String sqlForQuery = dbquery.getSQLForQuery();
		String testableQuery = testableSQL(sqlForQuery);
		System.out.println(sqlForQuery);

		final String marqueCondition = testableSQL("1997432637.ENABLED = 1");
		Assert.assertThat(testableQuery, containsString(marqueCondition));
		Assert.assertThat(testableQuery.indexOf(marqueCondition), is(testableQuery.lastIndexOf(marqueCondition)));

		if (database.supportsFullOuterJoin()) {
			List<DBQueryRow> allRows = dbquery.getAllRows();
			database.print(allRows);
			Assert.assertThat(allRows.size(), is(26));
		}
	}

	@Test
	public void testExpressionsInLeftOuterOnClause() {
		DBQuery query = database.getDBQuery();
		CarCompany carCompany = new CarCompany();
		Marque marque = new Marque();

		query.add(carCompany);
		query.addCondition(carCompany.column(carCompany.name).is("BREAD"));

		query.addOptional(marque);
		query.addCondition(marque.column(marque.auto_created).isLessThan("YEAH"));

		String sqlForQuery = query.getSQLForQuery();
		System.out.println(sqlForQuery);
		Assert.assertThat(sqlForQuery.indexOf("BREAD"), greaterThan(0));
		Assert.assertThat(sqlForQuery.indexOf("YEAH"), is(sqlForQuery.lastIndexOf("YEAH")));

	}
}
