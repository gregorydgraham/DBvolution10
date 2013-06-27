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
package nz.co.gregs.dbvolution.h2;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import static junit.framework.TestCase.assertTrue;
import nz.co.gregs.dbvolution.DBTableRow;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.operators.DBExistsOperator;

/**
 *
 * @author gregory.graham
 */
public class DBExistsOperatorTest extends AbstractTest {

    public DBExistsOperatorTest(String name) {
        super(name);
    }

    public void testDBExistsOperator() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException, ClassNotFoundException {
        Marque marque = new Marque();
        CarCompany carCompany = new CarCompany();
        carCompany.uidCarCompany.isLiterally(3);
        DBExistsOperator op = new DBExistsOperator(carCompany, "UID_CARCOMPANY");
        marque.getCarCompany().setOperator(op);
        marques.getByExample(marque);
        for (DBTableRow row : marques) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", marques.size() == 3);
    }
}
