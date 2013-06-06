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

    public void testQueryGeneration() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException {
        DBQuery dbQuery = new DBQuery(myDatabase);
        CarCompany carCompany = new CarCompany();
        carCompany.name.isLiterally("TOYOTA");
        dbQuery.add(carCompany);
        dbQuery.add(new Marque());
        final String generateSQLString = dbQuery.generateSQLString().replaceAll(" +", " ");


        String expectedResult = "select car_company.name, car_company.uid_carcompany, marque.numeric_code, marque.uid_marque, marque.isusedfortafros, marque.fk_toystatusclass, marque.intindallocallowed, marque.upd_count, marque.auto_created, marque.name, marque.pricingcodeprefix, marque.reservationsalwd, marque.creation_date, marque.fk_carcompany from car_company, marque where 1=1 and CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY;";
        System.out.println(expectedResult);
        System.out.println(generateSQLString);
        assertEquals(expectedResult, generateSQLString);

//        List<DBQueryResults>results = dbQuery.getResults();
//        results.printAll();

    }
    public void testQueryExecution() throws IntrospectionException, IllegalArgumentException, InvocationTargetException, IllegalAccessException, SQLException {
        DBQuery dbQuery = new DBQuery(myDatabase);
        CarCompany carCompany = new CarCompany();
        carCompany.name.isLiterally("TOYOTA");
        dbQuery.add(carCompany);
        dbQuery.add(new Marque());
        final String generateSQLString = dbQuery.generateSQLString().replaceAll(" +", " ");


        List<Map<Class,DBTableRow>>results = dbQuery.getResults();
        //results.printAll();

    }
}
