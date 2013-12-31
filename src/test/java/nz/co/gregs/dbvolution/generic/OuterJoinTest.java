/*
 * Copyright 2013 gregorygraham.
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
package nz.co.gregs.dbvolution.generic;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.operators.DBEqualsIgnoreCaseOperator;
import nz.co.gregs.dbvolution.operators.DBGreaterThanOperator;
import nz.co.gregs.dbvolution.query.QueryGraph;
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

    @Test
    public void testANSIJoinClauseCreation() throws Exception {
        String lineSep = System.getProperty("line.separator");
        Marque mrq = new Marque();
//        mrq.setDatabase(database);
        CarCompany carCo = new CarCompany();
//        carCo.setDatabase(database);
        System.out.println("" + mrq.getRelationshipsAsSQL(database, carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(database, mrq));

        String expectedString = "__1997432637.fk_carcompany = __78874071.uid_carcompany";
        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL(expectedString)));
        expectedString = "__78874071.UID_CARCOMPANY = __1997432637.FK_CARCOMPANY";
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL(expectedString)));

//        mrq.ignoreAllForeignKeys();
        mrq.addRelationship(mrq.name, carCo, carCo.name, new DBEqualsIgnoreCaseOperator());
        System.out.println("" + mrq.getRelationshipsAsSQL(database, carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(database, mrq));

        expectedString = "__1997432637.FK_CARCOMPANY = __78874071.UID_CARCOMPANY" + lineSep + " and  lower(__1997432637.NAME) =  lower(__78874071.NAME)";
        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL(expectedString)));
        expectedString = "lower(__78874071.NAME) =  lower(__1997432637.NAME)" + lineSep + " and __78874071.UID_CARCOMPANY = __1997432637.FK_CARCOMPANY";
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL(expectedString)));

        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL("__1997432637.FK_CARCOMPANY = __78874071.UID_CARCOMPANY and lower(__1997432637.NAME) = lower(__78874071.NAME)")));
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL("lower(__78874071.NAME) = lower(__1997432637.NAME) and __78874071.UID_CARCOMPANY = __1997432637.FK_CARCOMPANY")));

        mrq.ignoreAllForeignKeys();
        System.out.println("" + mrq.getRelationshipsAsSQL(database, carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(database, mrq));
        expectedString = "lower(__1997432637.NAME) =  lower(__78874071.NAME)";
        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL(expectedString)));
        expectedString = "lower(__78874071.NAME) =  lower(__1997432637.NAME)";
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL(expectedString)));

        mrq.addRelationship(mrq.name, carCo, carCo.name, new DBGreaterThanOperator());
        System.out.println("" + mrq.getRelationshipsAsSQL(database, carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(database, mrq));
        expectedString = "lower(__1997432637.NAME) =  lower(__78874071.NAME)" + lineSep + " and __1997432637.NAME > __78874071.NAME";
        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL(expectedString)));
        expectedString = "lower(__78874071.NAME) =  lower(__1997432637.NAME)" + lineSep + " and __78874071.NAME <= __1997432637.NAME";
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL(expectedString)));

    }

    @Test
    public void testANSIInnerJoinQueryCreation() throws Exception {
        Marque mrq = new Marque();
        CarCompany carCo = new CarCompany();
        LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();
        CompanyLogo logo = new CompanyLogo();

        DBQuery dbQuery = database.getDBQuery(mrq);
        List<DBRow> tables = new ArrayList<DBRow>();
        QueryGraph queryGraph = new QueryGraph();
        StringBuilder ansiJoinClause = new StringBuilder();
        ansiJoinClause.append(dbQuery.getANSIJoinClause(carCo, tables, queryGraph));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedCarCoJoin = "car_company as __78874071";
        Assert.assertThat(testableSQL(ansiJoinClause.toString()),
                is(testableSQL(expectedCarCoJoin)));

        tables.add(carCo);
        queryGraph = new QueryGraph();
        ansiJoinClause.append(dbQuery.getANSIJoinClause(mrq, tables, queryGraph));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedMarqueJoin = "car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany )";
        Assert.assertThat(
                testableSQL(ansiJoinClause.toString()),
                is(testableSQL(expectedMarqueJoin)));


        tables.add(mrq);
        queryGraph = new QueryGraph();
        ansiJoinClause.append(dbQuery.getANSIJoinClause(link, tables, queryGraph));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedLinkJoin = "car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company )";
        Assert.assertThat(
                testableSQL(ansiJoinClause.toString()),
                is(testableSQL(expectedLinkJoin)));



        tables.add(link);
        queryGraph = new QueryGraph();
        ansiJoinClause.append(dbQuery.getANSIJoinClause(logo, tables, queryGraph));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedLogoJoin = "car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) inner join companylogo as _1159239592 on( __78874071.uid_carcompany = _1159239592.car_company_fk and _1617907935.fk_company_logo = _1159239592.logo_id )";
        Assert.assertThat(
                testableSQL(ansiJoinClause.toString()),
                is(testableSQL(expectedLogoJoin)));
    }

    @Test
    public void testANSIQueryCreation() throws SQLException, Exception {
        DBQuery dbQuery = database.getDBQuery(new CarCompany(), new Marque());
        dbQuery.setUseANSISyntax(true);
        dbQuery.setBlankQueryAllowed(true);
        String sqlForQuery = dbQuery.getSQLForQuery();
        String expected2TableQuery =
                "select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) where 1=1 ;";
        System.out.println(sqlForQuery);
        Assert.assertThat(
                testableSQLWithoutColumnAliases(sqlForQuery),
                is(testableSQLWithoutColumnAliases(expected2TableQuery)));
        Assert.assertThat(dbQuery.count(), is(22L));
        dbQuery.printAllDataColumns(System.out);
        LinkCarCompanyAndLogo linkCoAndLogo = new LinkCarCompanyAndLogo();

        dbQuery.add(linkCoAndLogo);
        sqlForQuery = dbQuery.getSQLForQuery();
        String expected3TableQuery =
                "select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) inner join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) where 1=1 ;";
        System.out.println(sqlForQuery);
        Assert.assertThat(
                testableSQLWithoutColumnAliases(sqlForQuery),
                is(testableSQLWithoutColumnAliases(expected3TableQuery)));
        Assert.assertThat(dbQuery.count(), is(0L));
        dbQuery.printAllDataColumns(System.out);

        dbQuery.remove(linkCoAndLogo);
        dbQuery.addOptional(linkCoAndLogo);
        sqlForQuery = dbQuery.getSQLForQuery();
        String expected1OptionalTableQuery =
                "select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) left outer join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) where 1=1 ;";
        System.out.println(sqlForQuery);
        Assert.assertThat(
                testableSQLWithoutColumnAliases(sqlForQuery),
                is(testableSQLWithoutColumnAliases(expected1OptionalTableQuery)));
        dbQuery.print(System.out);
        Assert.assertThat(dbQuery.count(), is(22L));

        dbQuery.addOptional(new CompanyLogo());
        sqlForQuery = dbQuery.getSQLForQuery();
        String expected2OptionalTableQuery =
                "select __78874071.name, __78874071.uid_carcompany, __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from car_company as __78874071 inner join marque as __1997432637 on( __78874071.uid_carcompany = __1997432637.fk_carcompany ) left outer join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) left outer join companylogo as _1159239592 on( __78874071.uid_carcompany = _1159239592.car_company_fk and _1617907935.fk_company_logo = _1159239592.logo_id ) where 1=1 ;";
        System.out.println(sqlForQuery);
        Assert.assertThat(
                testableSQLWithoutColumnAliases(sqlForQuery),
                is(testableSQLWithoutColumnAliases(expected2OptionalTableQuery)));
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
        String expectedFullOuterQuery =
                "select __1997432637.numeric_code, __1997432637.uid_marque, __1997432637.isusedfortafros, __1997432637.fk_toystatusclass, __1997432637.intindallocallowed, __1997432637.upd_count, __1997432637.auto_created, __1997432637.name, __1997432637.pricingcodeprefix, __1997432637.reservationsalwd, __1997432637.creation_date, __1997432637.enabled, __1997432637.fk_carcompany, __78874071.name, __78874071.uid_carcompany, _1617907935.fk_car_company, _1617907935.fk_company_logo, _1159239592.logo_id, _1159239592.car_company_fk, _1159239592.image_file, _1159239592.image_name from marque as __1997432637 full outer join car_company as __78874071 on( __1997432637.fk_carcompany = __78874071.uid_carcompany ) full outer join lt_carco_logo as _1617907935 on( __78874071.uid_carcompany = _1617907935.fk_car_company ) full outer join companylogo as _1159239592 on( __78874071.uid_carcompany = _1159239592.car_company_fk and _1617907935.fk_company_logo = _1159239592.logo_id ) where 1=1 ;";
        System.out.println(testableSQLWithoutColumnAliases(sqlForQuery));
        System.out.println(testableSQLWithoutColumnAliases(expectedFullOuterQuery));
        Assert.assertThat(
                testableSQLWithoutColumnAliases(sqlForQuery),
                is(testableSQLWithoutColumnAliases(expectedFullOuterQuery)));
        // FULL OUTER JOIN not supported by H2 or MySqldb
        if (!
                ((database instanceof H2DB) || (database instanceof MySQLDB))
                ) {
            dbQuery.print(System.out);
            Assert.assertThat(dbQuery.count(), is(22L));
        }
    }
}
