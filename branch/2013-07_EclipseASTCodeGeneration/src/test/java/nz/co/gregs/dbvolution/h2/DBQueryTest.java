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

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.List;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;

/**
 *
 * @author gregorygraham
 */
public class DBQueryTest extends AbstractTest {

    public DBQueryTest(String name) {
        super(name);
    }

    public void testQueryGeneration() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        DBQuery dbQuery = new DBQuery(myDatabase);
        CarCompany carCompany = new CarCompany();
        carCompany.name.isLiterally("TOYOTA");
        dbQuery.add(carCompany);
        dbQuery.add(new Marque());
        final String generateSQLString = dbQuery.generateSQLString().replaceAll(" +", " ");


        String expectedResult = "select CAR_COMPANY.NAME, \n"
                + "CAR_COMPANY.UID_CARCOMPANY, \n"
                + "MARQUE.NUMERIC_CODE, \n"
                + "MARQUE.UID_MARQUE, \n"
                + "MARQUE.ISUSEDFORTAFROS, \n"
                + "MARQUE.FK_TOYSTATUSCLASS, \n"
                + "MARQUE.INTINDALLOCALLOWED, \n"
                + "MARQUE.UPD_COUNT, \n"
                + "MARQUE.AUTO_CREATED, \n"
                + "MARQUE.NAME, \n"
                + "MARQUE.PRICINGCODEPREFIX, \n"
                + "MARQUE.RESERVATIONSALWD, \n"
                + "MARQUE.CREATION_DATE, \n"
                + "MARQUE.FK_CARCOMPANY from car_company, \n"
                + "marque where 1=1 and CAR_COMPANY.NAME = 'TOYOTA' \n"
                + "and CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY;";

        System.out.println(expectedResult);
        System.out.println(generateSQLString);
        assertEquals(expectedResult.replaceAll("\\s+", " "), generateSQLString.replaceAll("\\s+", " "));
    }

    public void testQueryExecution() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, SQLException, SQLException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        DBQuery dbQuery = new DBQuery(myDatabase);
        CarCompany carCompany = new CarCompany();
        carCompany.name.isLiterally("TOYOTA");
        dbQuery.add(carCompany);
        dbQuery.add(new Marque());

        List<DBQueryRow> results = dbQuery.getAllRows();
        dbQuery.printRows();
        assertEquals(2, results.size());

        for (DBQueryRow queryRow : results) {
            
            CarCompany carCo = (CarCompany) queryRow.get(carCompany);
            String carCoName = carCo.name.toString();

            Marque marque = (Marque) queryRow.get(new Marque());
            Long marqueUID = marque.getUidMarque().longValue();

            System.out.println(carCoName + ": " + marqueUID);
            assertTrue(carCoName.equals("TOYOTA"));
            assertTrue(marqueUID == 1 || marqueUID == 4896300);
        }
    }

    public void testQuickQueryCreation() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, SQLException, SQLException, InstantiationException, NoSuchMethodException, ClassNotFoundException {

        CarCompany carCompany = new CarCompany();
        carCompany.name.isLiterally("TOYOTA");
        DBQuery dbQuery = new DBQuery(myDatabase, carCompany, new Marque());

        List<DBQueryRow> results = dbQuery.getAllRows();
        System.out.println(dbQuery.generateSQLString());
        dbQuery.printRows();
        assertEquals(2, results.size());

        for (DBQueryRow queryRow : results) {
            
            CarCompany carCo = (CarCompany) queryRow.get(carCompany);
            String carCoName = carCo.name.toString();

            Marque marque = (Marque) queryRow.get(new Marque());
            Long marqueUID = marque.getUidMarque().longValue();

            System.out.println(carCoName + ": " + marqueUID);
            assertTrue(carCoName.equals("TOYOTA"));
            assertTrue(marqueUID == 1 || marqueUID == 4896300);
        }
    }

    public void testDBTableRowReuse() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, SQLException, SQLException, InstantiationException, NoSuchMethodException, ClassNotFoundException {

        CarCompany carCompany = new CarCompany();
        carCompany.name.isLiterally("TOYOTA");
        DBQuery dbQuery = new DBQuery(myDatabase, carCompany, new Marque());

        List<DBQueryRow> results = dbQuery.getAllRows();
        System.out.println(dbQuery.generateSQLString());
        dbQuery.printRows();
        assertEquals(2, results.size());

        DBQueryRow[] rows = new DBQueryRow[2];
        rows = results.toArray(rows);

        CarCompany firstCarCo = (CarCompany) rows[0].get(carCompany);
        CarCompany secondCarCo = (CarCompany) rows[1].get(carCompany);
        assertTrue(firstCarCo == secondCarCo);
    }
}
