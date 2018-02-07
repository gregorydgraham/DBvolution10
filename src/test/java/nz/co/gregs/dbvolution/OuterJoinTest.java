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

import nz.co.gregs.dbvolution.internal.query.QueryState;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.DBAutoIncrement;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.exceptions.AutoCommitActionDuringTransactionException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OuterJoinTest extends AbstractTest {

	private boolean tablesCreated = false;

	public OuterJoinTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testANSIInnerJoinQueryCreation() throws Exception {
		Marque mrq = new Marque();
		CarCompany carCo = new CarCompany();
		LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();
		CompanyLogo logo = new CompanyLogo();

		DBQuery dbQuery = database.getDBQuery(mrq);
		final QueryState queryState = new QueryState(dbQuery.getQueryDetails());
		StringBuilder ansiJoinClause = new StringBuilder();
		final DBDefinition defn = database.getDefinition();
		ansiJoinClause.append(dbQuery.getQueryDetails().getANSIJoinClause(defn, queryState, carCo, new QueryOptions()));

		String expectedCarCoJoin = "car_company as __78874071";
		String expectedCarCoJoinOracle = "car_company __78874071";
		Assert.assertThat(testableSQL(ansiJoinClause.toString()),
				anyOf(
						is(testableSQL(expectedCarCoJoin)),
						is(testableSQL(expectedCarCoJoinOracle))
				));

		queryState.addJoinedTable(carCo);
		ansiJoinClause.append(dbQuery.getQueryDetails().getANSIJoinClause(defn, queryState, mrq, new QueryOptions()));

		String expectedMarqueJoin1 = "car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany )";
		String expectedMarqueJoin2 = "car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany )";
		Assert.assertThat(
				testableSQL(ansiJoinClause.toString()),
				anyOf(
						is(testableSQL(expectedMarqueJoin1)),
						is(testableSQL(expectedMarqueJoin2))
				));

		queryState.addJoinedTable(mrq);
		ansiJoinClause.append(dbQuery.getQueryDetails().getANSIJoinClause(defn, queryState, link, new QueryOptions()));

		String expectedLinkJoin = "car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company )";
		Assert.assertThat(
				testableSQL(ansiJoinClause.toString()),
				anyOf(
						is(testableSQL(expectedLinkJoin)),
						is(testableSQL("car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany )"))
				));

		queryState.addJoinedTable(link);
		ansiJoinClause.append(dbQuery.getQueryDetails().getANSIJoinClause(defn, queryState, logo, new QueryOptions()));

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

		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(is(testableSQLWithoutColumnAliases(expected2TableQuery1)), is(testableSQLWithoutColumnAliases(expected2TableQuery2))));
		Assert.assertThat(dbQuery.count(), is(22L));

		LinkCarCompanyAndLogo linkCoAndLogo = new LinkCarCompanyAndLogo();

		dbQuery.add(linkCoAndLogo);
		sqlForQuery = dbQuery.getSQLForQuery();
		String expected3TableQuery
				= testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) ;");
		String otherQueryVersion = testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) ;");

		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(is(expected3TableQuery),
						is(otherQueryVersion),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) inner join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) ;"))
				));
		Assert.assertThat(dbQuery.count(), is(0L));

		dbQuery.remove(linkCoAndLogo);
		dbQuery.addOptional(linkCoAndLogo);
		sqlForQuery = dbQuery.getSQLForQuery();
		String expected1OptionalTableQuery
				= testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) left outer join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) ;");
		String expected1OptionalTableQuery2
				= testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 left outer join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) ;");
		String expected1OptionalTableQuery3
				= testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) ;");

		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(is(expected1OptionalTableQuery),
						is(expected1OptionalTableQuery2),
						is(expected1OptionalTableQuery3),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) ;"))
				));
		Assert.assertThat(dbQuery.count(), is(22L));

		dbQuery.addOptional(new CompanyLogo());
		sqlForQuery = dbQuery.getSQLForQuery();

		Assert.assertThat(
				testableSQLWithoutColumnAliases(sqlForQuery),
				anyOf(
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from car_company as __78874071 inner join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) left outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;")),
						is(testableSQLWithoutColumnAliases("select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from car_company __78874071 inner join marque __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) left outer join companylogo _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id )"))
				));

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

		// FULL OUTER JOIN not supported natively by H2, SQLite, or MySqldb
		if (database.getDefinition().supportsFullOuterJoin()) {
			String sqlForQuery = dbQuery.getSQLForQuery();
			Assert.assertThat(dbQuery.count(), is(22L));

			String expectedFullOuterQuery = testableSQLWithoutColumnAliases("select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name, _1617907935.fk_car_company, _1617907935.fk_company_logo from marque as __1997432637 full outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) full outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) full outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;");
			String otherExpectedFullOuterQuery = testableSQLWithoutColumnAliases("select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from marque as __1997432637 full outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) full outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) full outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;");
			String pseudoFullOuterQuery1 = testableSQLWithoutColumnAliases("select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from marque as __1997432637 left outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) left outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) union distinct select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from marque as __1997432637 right outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) right outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) right outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;");
			String pseudoFullOuterQuery2 = testableSQLWithoutColumnAliases("select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name, _1617907935.fk_car_company, _1617907935.fk_company_logo from marque as __1997432637 left outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) union distinct select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name, _1617907935.fk_car_company, _1617907935.fk_company_logo from marque as __1997432637 right outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) right outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) right outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) ;");
			String reversedFullOuterQuery1 = testableSQLWithoutColumnAliases("select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from marque as __1997432637 left outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany ) left outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) union select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from companylogo as _1159239592 left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_company_logo = _1159239592.logo_id ) left outer join car_company as __78874071 on( _1159239592.car_company_fk = __78874071.uid_carcompany and _1617907935.fk_car_company = __78874071.uid_carcompany ) left outer join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany )");
			String reversedFullOuterQuery2 = testableSQLWithoutColumnAliases("select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name, _1617907935.fk_car_company, _1617907935.fk_company_logo from marque as __1997432637 left outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) left outer join companylogo as _1159239592 on( _1159239592.car_company_fk = __78874071.uid_carcompany ) left outer join lt_carco_logo as _1617907935 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1617907935.fk_company_logo = _1159239592.logo_id ) union select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name, _1617907935.fk_car_company, _1617907935.fk_company_logo from lt_carco_logo as _1617907935 left outer join companylogo as _1159239592 on( _1617907935.fk_company_logo = _1159239592.logo_id ) left outer join car_company as __78874071 on( _1617907935.fk_car_company = __78874071.uid_carcompany and _1159239592.car_company_fk = __78874071.uid_carcompany ) left outer join marque as __1997432637 on( __1997432637.fk_carcompany = __78874071.uid_carcompany )");
				Assert.assertThat(
						testableSQLWithoutColumnAliases(sqlForQuery),
						anyOf(
								is(expectedFullOuterQuery),
								is(otherExpectedFullOuterQuery),
								is(pseudoFullOuterQuery1),
								is(pseudoFullOuterQuery2),
								is(reversedFullOuterQuery1),
								is(reversedFullOuterQuery2)
						)
				);

//			if (database.getDefinition().supportsFullOuterJoinNatively()) {
//				Assert.assertThat(
//						testableSQLWithoutColumnAliases(sqlForQuery),
//						anyOf(
//								is(expectedFullOuterQuery),
//								is(otherExpectedFullOuterQuery)
//						)
//				);
//			} else if (database.getDefinition().supportsUnionDistinct()) {
//				Assert.assertThat(
//						testableSQLWithoutColumnAliases(sqlForQuery),
//						anyOf(
//								is(pseudoFullOuterQuery1),
//								is(pseudoFullOuterQuery2)
//						)
//				);
//			} else {
//				Assert.assertThat(
//						testableSQLWithoutColumnAliases(sqlForQuery),
//						anyOf(
//								is(reversedFullOuterQuery1),
//								is(reversedFullOuterQuery2))
//				);
//			}
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
		// FULL OUTER JOIN not supported natively by H2, MySQL, or SQLite
		if (database.getDefinition().supportsFullOuterJoin()) {
			String sqlForQuery = dbQuery.getSQLForQuery();
			Assert.assertThat(
					testableSQLWithoutColumnAliases(sqlForQuery),
					not(containsString("( 1=1 )")));
			Assert.assertThat(dbQuery.count(), is(22L));
		}
	}

	@Test
	public void testOuterJoinQueryCreatesEmptyRows() throws SQLException {
		DBQuery dbQuery = database.getDBQuery();

		dbQuery.add(new CarCompany());
		dbQuery.addOptional(new LinkCarCompanyAndLogo());
		dbQuery.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = dbQuery.getAllRows();
		Assert.assertThat(allRows.size(), is(4));
		List<LinkCarCompanyAndLogo> links = dbQuery.getAllInstancesOf(new LinkCarCompanyAndLogo());

		Assert.assertThat(links.size(), is(1));
		for (DBQueryRow queryrow : allRows) {
			Assert.assertThat(queryrow.get(new LinkCarCompanyAndLogo()).isEmptyRow(), is(true));
		}
	}

	@Test
	public void testSimpleCriteriaInOnClause() throws SQLException {
		DBQuery dbquery = database.getDBQuery();
		final CarCompany carCompany = new CarCompany();
		carCompany.name.permittedRangeInclusive("Ford", "TOYOTA");
		dbquery.add(carCompany);
		final Marque marque = new Marque();
		marque.enabled.permittedValues(true);
		dbquery.addOptional(marque);
		String sqlForQuery = dbquery.getSQLForQuery();

		final String marqueCondition = "__1997432637.ENABLED = 1";
		Assert.assertThat(sqlForQuery.indexOf(marqueCondition), is(sqlForQuery.lastIndexOf(marqueCondition)));
		List<DBQueryRow> allRows = dbquery.getAllRows();

		dbquery = database.getDBQuery();
		dbquery.add(marque);
		dbquery.addOptional(carCompany);
		sqlForQuery = dbquery.getSQLForQuery();

		Assert.assertThat(testableSQL(sqlForQuery),
				allOf(
						anyOf(
								containsString(testableSQL("(__78874071.name) >= 'ford'")),
								containsString(testableSQL("(__78874071.name) >= N'ford'"))),
						anyOf(
								containsString(testableSQL("(__78874071.name) <= 'toyota'")),
								containsString(testableSQL("(__78874071.name) <= N'toyota'"))),
						anyOf(containsString(testableSQL("__1997432637.enabled = TRUE")),
								containsString(testableSQL("__1997432637.enabled = 1")),
								containsString(testableSQL("( CASE WHEN __1997432637.enabled IS NULL THEN -1 ELSE __1997432637.enabled END ) = ( CASE WHEN  1  IS NULL THEN -1 ELSE  1  END )")))
				));
		final String carCompanyCondition = testableSQL("__78874071.NAME) >= 'ford' and (__78874071.NAME) <= 'TOYOTA'");
		final String testableQuery = testableSQL(sqlForQuery);
		Assert.assertThat(testableQuery.indexOf(carCompanyCondition), is(testableQuery.lastIndexOf(carCompanyCondition)));
	}

	@Test
	public void testFullOuterJoinWithSimpleCriteria() throws SQLException {
		DBQuery dbquery = database.getDBQuery();

		final CarCompany carCompany = new CarCompany();
		final Marque marque = new Marque();

		carCompany.name.permittedRangeInclusive("Ford", "TOYOTA");
		marque.enabled.permittedValues(Boolean.TRUE);

		dbquery.addOptional(marque);
		dbquery.addOptional(carCompany);

		if (database.getDefinition().supportsFullOuterJoin()) {
			String sqlForQuery = dbquery.getSQLForQuery();
			String testableQuery = testableSQL(sqlForQuery);

			final String marqueCondition = testableSQL("1997432637.ENABLED = TRUE");
			final String marqueConditionForDBsWithoutBooleans = testableSQL("1997432637.ENABLED = 1");
			Assert.assertThat(testableQuery,
					anyOf(containsString(marqueCondition),
							containsString(marqueConditionForDBsWithoutBooleans),
							containsString(testableSQL("( CASE WHEN __1997432637.ENABLED IS NULL THEN -1 ELSE __1997432637.ENABLED END ) = ( CASE WHEN  1  IS NULL THEN -1 ELSE  1  END ))"))
					)
			);

			List<DBQueryRow> allRows = dbquery.getAllRows();
			Assert.assertThat(allRows.size(), is(24));
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

		Assert.assertThat(sqlForQuery.indexOf("BREAD"), greaterThan(0));
		Assert.assertThat(sqlForQuery.indexOf("YEAH"), is(sqlForQuery.lastIndexOf("YEAH")));

	}

	@Test
	public void testSimpleLeftOuterJoin() throws SQLException {
		//Create some examples
		final CarCompany carCompany = new CarCompany();
		carCompany.name.permittedRangeInclusive("Ford", "TOYOTA");

		final Marque marque = new Marque();
		marque.enabled.permittedValues(true);

		//Create the query with the required table
		DBQuery dbquery = database.getDBQuery(carCompany).setBlankQueryAllowed(true);

		//Add the option table
		dbquery.addOptional(marque);

		// get the data
		List<DBQueryRow> allRows = dbquery.getAllRows();

		Assert.assertThat(allRows.size(), is(4));
	}

	@Test
	public void demonstrateAccessingEachRowWithOptionalTables() throws SQLException {
		// We're going to list all of the encounters that earned 100 or more experience
		// and the antagonist involved
		// So we need example Encounter and Antagonist objects
		final Encounter encounterExample = new Encounter();
		final Antagonist antagonistExample = new Antagonist();

		// limit the results to only encounters with 100 or more experienceEarned
		encounterExample.experienceEarned.permittedRangeInclusive(100, null);

		// We'll need a DBQuery so we can use more than one table.
		nz.co.gregs.dbvolution.DBQuery dbQuery
				= database.getDBQuery(encounterExample);

		// and add an instance of Antagonist to the mix
		// Optional means we don't require this table to have 
		// data connecting it to the rest of the query, 
		// DBvolution will create an outer join to accomodate our instruction
		dbQuery.addOptional(antagonistExample);

		// getting all the rows is similar to DBTable 
		// but returns a collection of DBQueryRows
		List<nz.co.gregs.dbvolution.DBQueryRow> allQueryRows
				= dbQuery.getAllRows();

		Assert.assertThat(allQueryRows.size(), is(3));

		// Now loop through the individual rows processing as you go
		int antagonistFound = 0;
		for (nz.co.gregs.dbvolution.DBQueryRow queryRow : allQueryRows) {

			// A DBQueryRow contains all the individual rows from the table 
			// that are associated with each other.
			// Use the get method with an instance of the class you want to get
			// the relevant row.
			Encounter encounter = queryRow.get(encounterExample);
			// Using blank instances is also ok
			Antagonist antagonist = queryRow.get(new Antagonist());

			// Watch out though!  
			// Optional rows will be NULL if there is no relevant row
			if (antagonist != null && !antagonist.isEmptyRow()) {
				antagonistFound++;
			}
		}
		Assert.assertThat(antagonistFound, is(2));
	}

	@Before
	public void createTables() throws SQLException {
		try {
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new Encounter());
			database.preventDroppingOfTables(false);
			database.dropTableNoExceptions(new Antagonist());
			database.createTable(new Encounter());
			database.createTable(new Antagonist());
			tablesCreated = true;
		} catch (SQLException | AutoCommitActionDuringTransactionException ex) {
			; // An exception is thrown if the table already exists
		}
		// An exception is thrown if the table already exists

		// To avoid duplicated rows, 
		// only insert if the tables are freshly created
		if (tablesCreated) {

			//Create some antagonists to fight
			//Note that the antagonists will not have valid IDs 
			//until they're inserted into the database
			Antagonist goblin = new Antagonist("Goblin");
			Antagonist guard = new Antagonist("Guard");
			Antagonist dragon = new Antagonist("Dragon");
			database.insert(goblin, guard, dragon);

			//Now that the antagonists have been inserted 
			//they have valid IDs 
			//and we can create some encounters
			Encounter encounter1 = new Encounter("First encounter", goblin, 20, new Date());
			Encounter encounter2 = new Encounter("Taking the guardtower", guard, 100, new Date());
			Encounter encounter3 = new Encounter("Slaying the dragon", dragon, 30000, new Date());

			//This one doesn't have a antagonist, 
			//and will demonstrate outer joins later
			Encounter encounter4 = new Encounter("Defeating the trap", 300, new Date());
			database.insert(encounter1, encounter2, encounter3, encounter4);
		}
	}

	public static class Encounter extends DBRow {

		private static final long serialVersionUID = 1;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger encounter_pk = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn
		@DBForeignKey(Antagonist.class)
		public DBInteger antagonist = new DBInteger();

		@DBColumn
		public DBInteger experienceEarned = new DBInteger();

		@DBColumn
		public DBDate dateEncountered = new DBDate();

		public Encounter() {
			super();
		}

		public Encounter(String name, Integer exp, Date date) {
			super();
			this.name.setValue(name);
			this.experienceEarned.setValue(exp);
			this.dateEncountered.setValue(date);
		}

		public Encounter(String name, Antagonist anatagonist, Integer exp, Date date) {
			super();
			this.name.setValue(name);
			this.antagonist.setValue(anatagonist.antagonistID.intValue());
			this.experienceEarned.setValue(exp);
			this.dateEncountered.setValue(date);
		}

	}

	public static class Antagonist extends DBRow {

		private static final long serialVersionUID = 1;
		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger antagonistID = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn
		public DBBoolean npc = new DBBoolean();

		public Antagonist(String name) {
			super();
			this.name.setValue(name);
			this.npc.setValue(false);
		}

		public Antagonist() {
			super();
		}

		// Some examples of pre-defined selections
		public static class Dragon extends Antagonist {

			private static final long serialVersionUID = 1;

			{
				this.name.permittedPatternIgnoreCase("%dragon%");
			}
		}

		public static class NPC extends Antagonist {

			private static final long serialVersionUID = 1;

			{
				this.npc.permittedValues(Boolean.TRUE);
			}
		}

		public static class Monster extends Antagonist {

			private static final long serialVersionUID = 1;

			{
				this.npc.permittedValues(Boolean.FALSE);
			}
		}
	}
}
