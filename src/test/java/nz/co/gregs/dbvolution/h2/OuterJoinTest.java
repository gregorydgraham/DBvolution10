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
package nz.co.gregs.dbvolution.h2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.*;
import nz.co.gregs.dbvolution.operators.DBEqualsCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.operators.DBGreaterThanOperator;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

public class OuterJoinTest extends AbstractTest {

    @Test
    public void testANSIJoinClauseCreation() {
        String lineSep = System.getProperty("line.separator");
        Marque mrq = new Marque();
        mrq.setDatabase(database);
        CarCompany carCo = new CarCompany();
        carCo.setDatabase(database);
        System.out.println("" + mrq.getRelationshipsAsSQL(carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(mrq));

        //MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY
        Assert.assertThat(mrq.getRelationshipsAsSQL(carCo).trim(), is("MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY"));
        //CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY
        Assert.assertThat(carCo.getRelationshipsAsSQL(mrq).trim(), is("CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY"));

//        mrq.ignoreAllForeignKeys();
        mrq.addRelationship(mrq.name, carCo, carCo.name, new DBEqualsCaseInsensitiveOperator());
        System.out.println("" + mrq.getRelationshipsAsSQL(carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(mrq));


        Assert.assertThat(mrq.getRelationshipsAsSQL(carCo).trim(), is("MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY" + lineSep + " and  lower(MARQUE.NAME) =  lower(CAR_COMPANY.NAME)"));
        Assert.assertThat(carCo.getRelationshipsAsSQL(mrq).trim(), is("lower(CAR_COMPANY.NAME) =  lower(MARQUE.NAME)" + lineSep + " and CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY"));

        mrq.ignoreAllForeignKeys();
        System.out.println("" + mrq.getRelationshipsAsSQL(carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(mrq));
        Assert.assertThat(mrq.getRelationshipsAsSQL(carCo).trim(), is("lower(MARQUE.NAME) =  lower(CAR_COMPANY.NAME)"));
        Assert.assertThat(carCo.getRelationshipsAsSQL(mrq).trim(), is("lower(CAR_COMPANY.NAME) =  lower(MARQUE.NAME)"));

        mrq.addRelationship(mrq.name, carCo, carCo.name, new DBGreaterThanOperator());
        System.out.println("" + mrq.getRelationshipsAsSQL(carCo));
        System.out.println("" + carCo.getRelationshipsAsSQL(mrq));
        Assert.assertThat(mrq.getRelationshipsAsSQL(carCo).trim(), is("lower(MARQUE.NAME) =  lower(CAR_COMPANY.NAME)" + lineSep + " and MARQUE.NAME > CAR_COMPANY.NAME"));
        Assert.assertThat(carCo.getRelationshipsAsSQL(mrq).trim(), is("lower(CAR_COMPANY.NAME) =  lower(MARQUE.NAME)" + lineSep + " and CAR_COMPANY.NAME <= MARQUE.NAME"));

    }

    @Test
    public void testANSIInnerJoinQueryCreation() {
        Marque mrq = new Marque();
        CarCompany carCo = new CarCompany();
        LinkCarCompanyAndLogo link = new LinkCarCompanyAndLogo();
        CompanyLogo logo = new CompanyLogo();

        DBQuery dbQuery = database.getDBQuery(mrq);
        List<DBRow> tables = new ArrayList<DBRow>();
        Set<DBRow> connectedTables = new HashSet<DBRow>();
        StringBuilder ansiJoinClause = new StringBuilder();
        ansiJoinClause.append(dbQuery.getANSIJoinClause(carCo, tables, connectedTables));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedCarCoJoin = "car_company";
        Assert.assertThat(ansiJoinClause.toString().trim().toLowerCase(),
                is(expectedCarCoJoin.trim().toLowerCase()));

        tables.add(carCo);
        ansiJoinClause.append(dbQuery.getANSIJoinClause(mrq, tables, connectedTables));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedMarqueJoin = "car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY ) ";
        Assert.assertThat(
                ansiJoinClause.toString().trim().toLowerCase().replaceAll("[ \\r\\n]+", " "),
                is(expectedMarqueJoin.trim().toLowerCase().replaceAll("[ \\r\\n]+", " ")));


        tables.add(mrq);
        ansiJoinClause.append(dbQuery.getANSIJoinClause(link, tables, connectedTables));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedLinkJoin = " car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  INNER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY ) ";
        Assert.assertThat(
                ansiJoinClause.toString().trim().toLowerCase().replaceAll("[ \\r\\n]+", " "),
                is(expectedLinkJoin.trim().toLowerCase().replaceAll("[ \\r\\n]+", " ")));



        tables.add(link);
        ansiJoinClause.append(dbQuery.getANSIJoinClause(logo, tables, connectedTables));
        System.out.println("=============");
        System.out.println(ansiJoinClause);
        System.out.println("=============");
        String expectedLogoJoin = "car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  INNER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY )  INNER JOIN CompanyLogo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = COMPANYLOGO.CAR_COMPANY_FK and \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO = COMPANYLOGO.LOGO_ID ) ";
        Assert.assertThat(
                ansiJoinClause.toString().trim().toLowerCase().replaceAll("[ \\r\\n]+", " "),
                is(expectedLogoJoin.trim().toLowerCase().replaceAll("[ \\r\\n]+", " ")));
    }

    @Test
    public void testANSIQueryCreation() throws SQLException {
        DBQuery dbQuery = database.getDBQuery(new CarCompany(), new Marque());
        dbQuery.setUseANSISyntax(true);
        String sqlForQuery = dbQuery.getSQLForQuery();
        String expected2TableQuery =
                " SELECT CAR_COMPANY.NAME _1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY _819159114, \n"
                + "MARQUE.NUMERIC_CODE __570915006, \n"
                + "MARQUE.UID_MARQUE __768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS _1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS _551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED __1405397146, \n"
                + "MARQUE.UPD_COUNT _1497912790, \n"
                + "MARQUE.AUTO_CREATED _332721019, \n"
                + "MARQUE.NAME __1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX __443037310, \n"
                + "MARQUE.RESERVATIONSALWD __1860726622, \n"
                + "MARQUE.CREATION_DATE __1712481749, \n"
                + "MARQUE.ENABLED __637053442, \n"
                + "MARQUE.FK_CARCOMPANY _1664116480\n"
                + " FROM  car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
        System.out.println(sqlForQuery);
        Assert.assertThat(
                sqlForQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " "),
                is(expected2TableQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " ")));
        Assert.assertThat(dbQuery.count(), is(22L));
        dbQuery.printAllDataColumns(System.out);
        LinkCarCompanyAndLogo linkCoAndLogo = new LinkCarCompanyAndLogo();

        dbQuery.add(linkCoAndLogo);
        sqlForQuery = dbQuery.getSQLForQuery();
        String expected3TableQuery =
                " SELECT CAR_COMPANY.NAME _1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY _819159114, \n"
                + "MARQUE.NUMERIC_CODE __570915006, \n"
                + "MARQUE.UID_MARQUE __768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS _1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS _551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED __1405397146, \n"
                + "MARQUE.UPD_COUNT _1497912790, \n"
                + "MARQUE.AUTO_CREATED _332721019, \n"
                + "MARQUE.NAME __1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX __443037310, \n"
                + "MARQUE.RESERVATIONSALWD __1860726622, \n"
                + "MARQUE.CREATION_DATE __1712481749, \n"
                + "MARQUE.ENABLED __637053442, \n"
                + "MARQUE.FK_CARCOMPANY _1664116480, \n"
                + "LT_CARCO_LOGO.FK_CAR_COMPANY __1988359495, \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO _1707036998\n"
                + " FROM  car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  INNER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
        System.out.println(sqlForQuery);
        Assert.assertThat(
                sqlForQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " "),
                is(expected3TableQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " ")));
        Assert.assertThat(dbQuery.count(), is(0L));
        dbQuery.printAllDataColumns(System.out);

        dbQuery.remove(linkCoAndLogo);
        dbQuery.addOptional(linkCoAndLogo);
        sqlForQuery = dbQuery.getSQLForQuery();
        String expected1OptionalTableQuery =
                " SELECT CAR_COMPANY.NAME _1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY _819159114, \n"
                + "MARQUE.NUMERIC_CODE __570915006, \n"
                + "MARQUE.UID_MARQUE __768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS _1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS _551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED __1405397146, \n"
                + "MARQUE.UPD_COUNT _1497912790, \n"
                + "MARQUE.AUTO_CREATED _332721019, \n"
                + "MARQUE.NAME __1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX __443037310, \n"
                + "MARQUE.RESERVATIONSALWD __1860726622, \n"
                + "MARQUE.CREATION_DATE __1712481749, \n"
                + "MARQUE.ENABLED __637053442, \n"
                + "MARQUE.FK_CARCOMPANY _1664116480, \n"
                + "LT_CARCO_LOGO.FK_CAR_COMPANY __1988359495, \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO _1707036998\n"
                + " FROM  car_company  INNER JOIN marque ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY )  LEFT OUTER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
        System.out.println(sqlForQuery);
        Assert.assertThat(
                sqlForQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " "),
                is(expected1OptionalTableQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " ")));
        dbQuery.print(System.out);
        Assert.assertThat(dbQuery.count(), is(22L));

        dbQuery.addOptional(new CompanyLogo());
        sqlForQuery = dbQuery.getSQLForQuery();
        String expected2OptionalTableQuery =
                " SELECT CAR_COMPANY.NAME _1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY _819159114, \n"
                + "MARQUE.NUMERIC_CODE __570915006, \n"
                + "MARQUE.UID_MARQUE __768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS _1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS _551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED __1405397146, \n"
                + "MARQUE.UPD_COUNT _1497912790, \n"
                + "MARQUE.AUTO_CREATED _332721019, \n"
                + "MARQUE.NAME __1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX __443037310, \n"
                + "MARQUE.RESERVATIONSALWD __1860726622, \n"
                + "MARQUE.CREATION_DATE __1712481749, \n"
                + "MARQUE.ENABLED __637053442, \n"
                + "MARQUE.FK_CARCOMPANY _1664116480, \n"
                + "LT_CARCO_LOGO.FK_CAR_COMPANY __1988359495, \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO _1707036998, \n"
                + "COMPANYLOGO.LOGO_ID _1189023175, \n"
                + "COMPANYLOGO.CAR_COMPANY_FK _1247307962, \n"
                + "COMPANYLOGO.IMAGE_FILE _402667880, \n"
                + "COMPANYLOGO.IMAGE_NAME _402898551\n"
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
                sqlForQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " "),
                is(expected2OptionalTableQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " ")));
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
        String sqlForQuery = dbQuery.getSQLForQuery();
        System.out.println(sqlForQuery);
        String expectedFullOuterQuery =
                " SELECT MARQUE.NUMERIC_CODE __570915006, \n"
                + "MARQUE.UID_MARQUE __768788587, \n"
                + "MARQUE.ISUSEDFORTAFROS _1658455900, \n"
                + "MARQUE.FK_TOYSTATUSCLASS _551644671, \n"
                + "MARQUE.INTINDALLOCALLOWED __1405397146, \n"
                + "MARQUE.UPD_COUNT _1497912790, \n"
                + "MARQUE.AUTO_CREATED _332721019, \n"
                + "MARQUE.NAME __1359288114, \n"
                + "MARQUE.PRICINGCODEPREFIX __443037310, \n"
                + "MARQUE.RESERVATIONSALWD __1860726622, \n"
                + "MARQUE.CREATION_DATE __1712481749, \n"
                + "MARQUE.ENABLED __637053442, \n"
                + "MARQUE.FK_CARCOMPANY _1664116480, \n"
                + "CAR_COMPANY.NAME _1064314813, \n"
                + "CAR_COMPANY.UID_CARCOMPANY _819159114, \n"
                + "LT_CARCO_LOGO.FK_CAR_COMPANY __1988359495, \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO _1707036998, \n"
                + "COMPANYLOGO.LOGO_ID _1189023175, \n"
                + "COMPANYLOGO.CAR_COMPANY_FK _1247307962, \n"
                + "COMPANYLOGO.IMAGE_FILE _402667880, \n"
                + "COMPANYLOGO.IMAGE_NAME _402898551\n"
                + " FROM  marque  FULL OUTER JOIN car_company ON( \n"
                + "MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY )  FULL OUTER JOIN lt_carco_logo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = LT_CARCO_LOGO.FK_CAR_COMPANY )  FULL OUTER JOIN CompanyLogo ON( \n"
                + "CAR_COMPANY.UID_CARCOMPANY = COMPANYLOGO.CAR_COMPANY_FK and \n"
                + "LT_CARCO_LOGO.FK_COMPANY_LOGO = COMPANYLOGO.LOGO_ID ) \n"
                + " WHERE  1=1 \n"
                + "\n"
                + ";";
        Assert.assertThat(
                sqlForQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " "),
                is(expectedFullOuterQuery.trim().toLowerCase().replaceAll("[ \\r\\n]+", " ")));
        dbQuery.print(System.out);
        // FULL OUTER JOIN not supported by H2
//        Assert.assertThat(dbQuery.count(), is(22L));
    }
}
