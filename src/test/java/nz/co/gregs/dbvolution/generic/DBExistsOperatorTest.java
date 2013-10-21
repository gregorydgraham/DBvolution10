/*
 * Copyright 2013 gregory.graham.
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
import java.util.List;
import static junit.framework.TestCase.assertTrue;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.operators.DBExistsOperator;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class DBExistsOperatorTest extends AbstractTest {

    public DBExistsOperatorTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void testDBExistsOperator() throws SQLException {
        
        CarCompany carCompany = new CarCompany();
        carCompany.uidCarCompany.permittedValues(3);
        DBExistsOperator<CarCompany> carCompanyExists = new DBExistsOperator<CarCompany>(carCompany, carCompany.uidCarCompany);

        Marque marque = new Marque();
        marque.getCarCompany().setOperator(carCompanyExists);
        
        marques.getRowsByExample(marque);
        List<Marque> rowList = marques.toList();
        for (DBRow row : rowList) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", rowList.size() == 3);
        
        marque = new Marque();
        marque.getCarCompany().setOperator(new DBExistsOperator<CarCompany>(carCompany, carCompany.uidCarCompany));
        marque.getCarCompany().negateOperator();
        
        marques.getRowsByExample(marque);
        rowList = marques.toList();
        for (DBRow row : rowList) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", rowList.size() == 19);
    }
}
