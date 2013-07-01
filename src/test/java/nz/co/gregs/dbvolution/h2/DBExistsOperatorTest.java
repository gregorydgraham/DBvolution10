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
import java.util.ArrayList;
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
        
        CarCompany carCompany = new CarCompany();
        carCompany.uidCarCompany.isLiterally(3);
        DBExistsOperator carCompanyExists = new DBExistsOperator(carCompany, carCompany.uidCarCompany);

        Marque marque = new Marque();
        marque.getCarCompany().setOperator(carCompanyExists);
        
        marques.getByExample(marque);
        ArrayList<Marque> rowList = marques.toList();
        for (DBTableRow row : rowList) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", rowList.size() == 3);
        
        marque = new Marque();
        marque.getCarCompany().doesExist(carCompany, carCompany.uidCarCompany);
        
        marques.getByExample(marque);
        for (DBTableRow row : rowList) {
            System.out.println(row);
        }
        assertTrue("Incorrect number of marques retreived", rowList.size() == 3);
    }
}
