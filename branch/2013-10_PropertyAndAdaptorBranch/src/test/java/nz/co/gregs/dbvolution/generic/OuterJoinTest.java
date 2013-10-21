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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.operators.DBEqualsCaseInsensitiveOperator;
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

        String expectedString = "MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY";
        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL(expectedString)));
        expectedString = "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY";
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL(expectedString)));

//        mrq.ignoreAllForeignKeys();
        mrq.addRelationship(mrq.name, carCo, carCo.name, new DBEqualsCaseInsensitiveOperator());
        System.out.println("" + mrq.getRelationshipsAsSQL(database, carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(database, mrq));

        expectedString = "MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY" + lineSep + " and  lower(MARQUE.NAME) =  lower(CAR_COMPANY.NAME)";
        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL(expectedString)));
        expectedString = "lower(CAR_COMPANY.NAME) =  lower(MARQUE.NAME)" + lineSep + " and CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY";
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL(expectedString)));

        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL("MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY and lower(MARQUE.NAME) = lower(CAR_COMPANY.NAME)")));
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL("lower(CAR_COMPANY.NAME) = lower(MARQUE.NAME) and CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY")));

        mrq.ignoreAllForeignKeys();
        System.out.println("" + mrq.getRelationshipsAsSQL(database, carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(database, mrq));
        expectedString = "lower(MARQUE.NAME) =  lower(CAR_COMPANY.NAME)";
        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL(expectedString)));
        expectedString = "lower(CAR_COMPANY.NAME) =  lower(MARQUE.NAME)";
        Assert.assertThat(testableSQL(carCo.getRelationshipsAsSQL(database, mrq)), is(testableSQL(expectedString)));

        mrq.addRelationship(mrq.name, carCo, carCo.name, new DBGreaterThanOperator());
        System.out.println("" + mrq.getRelationshipsAsSQL(database, carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(database, mrq));
        expectedString = "lower(MARQUE.NAME) =  lower(CAR_COMPANY.NAME)" + lineSep + " and MARQUE.NAME > CAR_COMPANY.NAME";
        Assert.assertThat(testableSQL(mrq.getRelationshipsAsSQL(database, carCo)), is(testableSQL(expectedString)));
        expectedString = "lower(CAR_COMPANY.NAME) =  lower(MARQUE.NAME)" + lineSep + " and CAR_COMPANY.NAME <= MARQUE.NAME";
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
        String expectedCarCoJoin = "car_company";
        Assert.assertThat(testableSQL(ansiJoinClause.toString()),
                is(testableSQL(expectedCarCoJoin)));

        tables.add(carCo);
        queryGraph = new QueryGraph();
        ansiJoinClause.append(dbQuery.getANSIJoinClause(mrq, tables, queryGraph));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedMarqueJoin = "car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY ) ";
        Assert.assertThat(
                testableSQL(ansiJoinClause.toString()),
                is(testableSQL(expectedMarqueJoin)));


        tables.add(mrq);
        queryGraph = new QueryGraph();
        ansiJoinClause.append(dbQuery.getANSIJoinClause(link, tables, queryGraph));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedLinkJoin = " car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  INNER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY ) ";
        Assert.assertThat(
                testableSQL(ansiJoinClause.toString()),
                is(testableSQL(expectedLinkJoin)));



        tables.add(link);
        queryGraph = new QueryGraph();
        ansiJoinClause.append(dbQuery.getANSIJoinClause(logo, tables, queryGraph));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedLogoJoin = "car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  INNER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY )  INNER JOIN CompanyLogo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = COMPANYLOGO.CAR_COMPANY_FK and \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO = COMPANYLOGO.LOGO_ID ) ";
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
                " SELECT CAR_COMPANY.NAME DB1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY DB819159114, \n"
                + "MARQUE.NUMERIC_CODE DB_570915006, \n"
                + "MARQUE.UID_MARQUE DB_768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS DB1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS DB551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED DB_1405397146, \n"
                + "MARQUE.UPD_COUNT DB1497912790, \n"
                + "MARQUE.AUTO_CREATED DB332721019, \n"
                + "MARQUE.NAME DB_1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX DB_443037310, \n"
                + "MARQUE.RESERVATIONSALWD DB_1860726622, \n"
                + "MARQUE.CREATION_DATE DB_1712481749, \n"
                + "MARQUE.ENABLED DB_637053442, \n"
                + "MARQUE.FK_CARCOMPANY DB1664116480\n"
                + " FROM  car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
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
                " SELECT CAR_COMPANY.NAME DB1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY DB819159114, \n"
                + "MARQUE.NUMERIC_CODE DB_570915006, \n"
                + "MARQUE.UID_MARQUE DB_768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS DB1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS DB551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED DB_1405397146, \n"
                + "MARQUE.UPD_COUNT DB1497912790, \n"
                + "MARQUE.AUTO_CREATED DB332721019, \n"
                + "MARQUE.NAME DB_1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX DB_443037310, \n"
                + "MARQUE.RESERVATIONSALWD DB_1860726622, \n"
                + "MARQUE.CREATION_DATE DB_1712481749, \n"
                + "MARQUE.ENABLED DB_637053442, \n"
                + "MARQUE.FK_CARCOMPANY DB1664116480, \n"
                + "LT_CARCO_LOGO.FK_CAR_COMPANY DB_1988359495, \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO DB1707036998\n"
                + " FROM  car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  INNER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
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
                " SELECT CAR_COMPANY.NAME DB1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY DB819159114, \n"
                + "MARQUE.NUMERIC_CODE DB_570915006, \n"
                + "MARQUE.UID_MARQUE DB_768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS DB1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS DB551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED DB_1405397146, \n"
                + "MARQUE.UPD_COUNT DB1497912790, \n"
                + "MARQUE.AUTO_CREATED DB332721019, \n"
                + "MARQUE.NAME DB_1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX DB_443037310, \n"
                + "MARQUE.RESERVATIONSALWD DB_1860726622, \n"
                + "MARQUE.CREATION_DATE DB_1712481749, \n"
                + "MARQUE.ENABLED DB_637053442, \n"
                + "MARQUE.FK_CARCOMPANY DB1664116480, \n"
                + "LT_CARCO_LOGO.FK_CAR_COMPANY DB_1988359495, \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO DB1707036998\n"
                + " FROM  car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  LEFT OUTER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
        System.out.println(sqlForQuery);
        Assert.assertThat(
                testableSQLWithoutColumnAliases(sqlForQuery),
                is(testableSQLWithoutColumnAliases(expected1OptionalTableQuery)));
        dbQuery.print(System.out);
        Assert.assertThat(dbQuery.count(), is(22L));

        dbQuery.addOptional(new CompanyLogo());
        sqlForQuery = dbQuery.getSQLForQuery();
        String expected2OptionalTableQuery =
                " SELECT CAR_COMPANY.NAME DB1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY DB819159114, \n"
                + "MARQUE.NUMERIC_CODE DB_570915006, \n"
                + "MARQUE.UID_MARQUE DB_768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS DB1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS DB551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED DB_1405397146, \n"
                + "MARQUE.UPD_COUNT DB1497912790, \n"
                + "MARQUE.AUTO_CREATED DB332721019, \n"
                + "MARQUE.NAME DB_1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX DB_443037310, \n"
                + "MARQUE.RESERVATIONSALWD DB_1860726622, \n"
                + "MARQUE.CREATION_DATE DB_1712481749, \n"
                + "MARQUE.ENABLED DB_637053442, \n"
                + "MARQUE.FK_CARCOMPANY DB1664116480, \n"
                + "LT_CARCO_LOGO.FK_CAR_COMPANY DB_1988359495, \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO DB1707036998, \n"
                + "COMPANYLOGO.LOGO_ID DB1189023175, \n"
                + "COMPANYLOGO.CAR_COMPANY_FK DB1247307962, \n"
                + "COMPANYLOGO.IMAGE_FILE DB402667880, \n"
                + "COMPANYLOGO.IMAGE_NAME DB402898551\n"
                + " FROM  car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  LEFT OUTER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY )  LEFT OUTER JOIN CompanyLogo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = COMPANYLOGO.CAR_COMPANY_FK and \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO = COMPANYLOGO.LOGO_ID ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
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
                " SELECT MARQUE.NUMERIC_CODE DB_570915006, \n"
                + "MARQUE.UID_MARQUE DB_768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS DB1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS DB551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED DB_1405397146, \n"
                + "MARQUE.UPD_COUNT DB1497912790, \n"
                + "MARQUE.AUTO_CREATED DB332721019, \n"
                + "MARQUE.NAME DB_1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX DB_443037310, \n"
                + "MARQUE.RESERVATIONSALWD DB_1860726622, \n"
                + "MARQUE.CREATION_DATE DB_1712481749, \n"
                + "MARQUE.ENABLED DB_637053442, \n"
                + "MARQUE.FK_CARCOMPANY DB1664116480, \n"
                + "CAR_COMPANY.NAME DB1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY DB819159114, \n"
                + "LT_CARCO_LOGO.FK_CAR_COMPANY DB_1988359495, \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO DB1707036998, \n"
                + "COMPANYLOGO.LOGO_ID DB1189023175, \n"
                + "COMPANYLOGO.CAR_COMPANY_FK DB1247307962, \n"
                + "COMPANYLOGO.IMAGE_FILE DB402667880, \n"
                + "COMPANYLOGO.IMAGE_NAME DB402898551\n"
                + " FROM  marque  FULL OUTER JOIN car_company ON( \n"
                + "MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY )  FULL OUTER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY )  FULL OUTER JOIN CompanyLogo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = COMPANYLOGO.CAR_COMPANY_FK and \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO = COMPANYLOGO.LOGO_ID ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
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
