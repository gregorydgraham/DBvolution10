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
import java.util.Map;
import static junit.framework.TestCase.assertEquals;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBTableRow;
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


        String expectedResult = "select CAR_COMPANY.NAME, CAR_COMPANY.UID_CARCOMPANY, MARQUE.NUMERIC_CODE, MARQUE.UID_MARQUE, MARQUE.ISUSEDFORTAFROS, MARQUE.FK_TOYSTATUSCLASS, MARQUE.INTINDALLOCALLOWED, MARQUE.UPD_COUNT, MARQUE.AUTO_CREATED, MARQUE.NAME, MARQUE.PRICINGCODEPREFIX, MARQUE.RESERVATIONSALWD, MARQUE.CREATION_DATE, MARQUE.FK_CARCOMPANY from car_company, marque where 1=1 and CAR_COMPANY.NAME = 'TOYOTA' and CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY;";
        System.out.println(expectedResult);
        System.out.println(generateSQLString);
        assertEquals(expectedResult, generateSQLString);

//        List<DBQueryResults>results = dbQuery.getResults();
//        results.printAll();

    }
    public void testQueryExecution() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, SQLException, SQLException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        DBQuery dbQuery = new DBQuery(myDatabase);
        CarCompany carCompany = new CarCompany();
        carCompany.name.isLiterally("TOYOTA");
        dbQuery.add(carCompany);
        dbQuery.add(new Marque());
        final String generateSQLString = dbQuery.generateSQLString().replaceAll(" +", " ");


        List<DBQueryRow> results = dbQuery.getAllRows();
        dbQuery.printAll();

    }
}
