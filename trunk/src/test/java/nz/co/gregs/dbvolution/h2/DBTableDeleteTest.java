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
import java.util.List;
import junit.framework.TestCase;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;

/**
 *
 * @author gregory.graham
 */
public class DBTableDeleteTest extends AbstractTest {

    public DBTableDeleteTest(String testName) {
        super(testName);
    }

    // TODO add test methods here. The name must begin with 'test'. For example:
    // public void testHello() {}

    public void testDeleteListOfRows() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        marques.getAllRows();
        int originalSize = marques.size();
        System.out.println("marques.size()==" + marques.size());
        ArrayList<Marque> deleteList = new ArrayList<Marque>();
        for (Marque row : marques) {
            if (row.getIsUsedForTAFROs().toString().equals("False")) {
                deleteList.add(row);
            }
        }
        marques.delete(deleteList);
        marques.getAllRows();
        System.out.println("marques.size()==" + marques.size());
        assertTrue("All 'False' rows have not been deleted", originalSize - deleteList.size() == marques.size());
    }
}
