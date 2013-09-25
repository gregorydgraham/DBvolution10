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
import java.util.List;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.*;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.IncorrectDBRowInstanceSuppliedException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class DBQueryTest extends AbstractTest {

    public DBQueryTest(Object db) {
        super(db);
    }

    @Test
    public void testQueryGeneration() throws SQLException {
        DBQuery dbQuery = database.getDBQuery();
        CarCompany carCompany = new CarCompany();
        carCompany.name.permittedValues("TOYOTA");
        dbQuery.add(carCompany);
        dbQuery.add(new Marque());
        final String generateSQLString = dbQuery.getSQLForQuery().replaceAll(" +", " ");


        String expectedResult = " SELECT CAR_COMPANY.NAME _1064314813, \n"
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
                + "MARQUE.FK_CARCOMPANY _1664116480 FROM car_company, \n"
                + "marque WHERE 1=1 and CAR_COMPANY.NAME = 'TOYOTA' \n"
                + "and CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY ;";
        if (dbQuery.isUseANSISyntax()) {
            expectedResult = " SELECT CAR_COMPANY.NAME _1064314813, \n"
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
                    + " and CAR_COMPANY.NAME = 'TOYOTA' \n"
                    + "\n"
                    + ";";
        }

        System.out.println(expectedResult);
        System.out.println(generateSQLString);
        Assert.assertThat(expectedResult.replaceAll("\\s+", " ").replaceAll("_+[0-9]+", "").toLowerCase(), 
                is(generateSQLString.replaceAll("\\s+", " ").replaceAll("_+[0-9]+", "").toLowerCase()));
    }

    @Test
    public void testQueryExecution() throws SQLException {
        DBQuery dbQuery = database.getDBQuery();
        CarCompany carCompany = new CarCompany();
        carCompany.name.permittedValues("TOYOTA");
        dbQuery.add(carCompany);
        dbQuery.add(new Marque());

        List<DBQueryRow> results = dbQuery.getAllRows();
        dbQuery.print();
        assertEquals(2, results.size());

        for (DBQueryRow queryRow : results) {

            CarCompany carCo = queryRow.get(carCompany);
            String carCoName = carCo.name.toString();

            Marque marque = queryRow.get(new Marque());
            Long marqueUID = marque.getUidMarque().longValue();

            System.out.println(carCoName + ": " + marqueUID);
            assertTrue(carCoName.equals("TOYOTA"));
            assertTrue(marqueUID == 1 || marqueUID == 4896300);
        }
    }

    @Test
    public void quickQueryCreation() throws SQLException {

        CarCompany carCompany = new CarCompany();
        carCompany.name.permittedValues("TOYOTA");
        DBQuery dbQuery = database.getDBQuery(carCompany, new Marque());

        List<DBQueryRow> results = dbQuery.getAllRows();
        System.out.println(dbQuery.getSQLForQuery());
        dbQuery.print();
        assertEquals(2, results.size());

        for (DBQueryRow queryRow : results) {

            CarCompany carCo = queryRow.get(carCompany);
            String carCoName = carCo.name.toString();

            Marque marque = queryRow.get(new Marque());
            Long marqueUID = marque.getUidMarque().longValue();

            System.out.println(carCoName + ": " + marqueUID);
            assertTrue(carCoName.equals("TOYOTA"));
            assertTrue(marqueUID == 1 || marqueUID == 4896300);
        }
    }

    @Test
    public void testDBTableRowReuse() throws SQLException {
        CarCompany carCompany = new CarCompany();
        carCompany.name.permittedValues("TOYOTA");
        DBQuery dbQuery = database.getDBQuery(carCompany, new Marque());

        List<DBQueryRow> results = dbQuery.getAllRows();
        System.out.println(dbQuery.getSQLForQuery());
        dbQuery.print();
        assertEquals(2, results.size());

        DBQueryRow[] rows = new DBQueryRow[2];
        rows = results.toArray(rows);

        CarCompany firstCarCo = rows[0].get(carCompany);
        CarCompany secondCarCo = rows[1].get(carCompany);
        assertTrue(firstCarCo == secondCarCo);
    }

    @Test
    public void thrownExceptionIfTheForeignKeyFieldToBeIgnoredIsNotInTheInstance() throws Exception {
        Marque wrongMarque = new Marque();
        Marque marqueQuery = new Marque();
        marqueQuery.uidMarque.permittedValues(wrongMarque.uidMarque.longValue());

        DBQuery query = database.getDBQuery(marqueQuery, new CarCompany());
        try {
            marqueQuery.ignoreForeignKey(wrongMarque.carCompany);
            throw new RuntimeException("IncorrectDBRowInstanceSuppliedException should have been thrown");
        } catch (IncorrectDBRowInstanceSuppliedException wrongDBRowEx) {
        }
        marqueQuery.ignoreForeignKey(marqueQuery.carCompany);
        query.setCartesianJoinsAllowed(true);
        List<DBQueryRow> rows = query.getAllRows();
        System.out.println(query.getSQLForQuery());

        System.out.println();
        System.out.println(rows);
    }
}
