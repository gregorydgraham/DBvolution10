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
import nz.co.gregs.dbvolution.example.Marque;

/**
 *
 * @author gregory.graham
 */
public class DBTableDeleteTest extends TestCase {

    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolutionDeleteTest", "", "");
    Marque myTableRow = new Marque();
    DBTable<Marque> marques;

    public DBTableDeleteTest(String testName) {
        super(testName);
    }

    @Override
    @SuppressWarnings("empty-statement")
    protected void setUp() throws Exception {
        super.setUp();

        myDatabase.dropTableNoExceptions(new Marque());

        myDatabase.createTable(myTableRow);
        DBTable.setPrintSQLBeforeExecuting(false);
        marques = new DBTable<Marque>(myTableRow, myDatabase);

        List<Marque> myTableRows = new ArrayList<Marque>();
        myTableRows.add(new Marque(4893059, "True", 1246974, "", 3, "UV", "PEUGEOT", "", "Y"));
        myTableRows.add(new Marque(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y"));
        myTableRows.add(new Marque(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y"));
        myTableRows.add(new Marque(4893112, "False", 1246974, "", 2, "UV", "MITSUBISHI", "", "Y"));
        myTableRows.add(new Marque(4893150, "False", 1246974, "", 3, "UV", "SUZUKI", "", "Y"));
        myTableRows.add(new Marque(4893263, "False", 1246974, "", 2, "UV", "HONDA", "", "Y"));
        myTableRows.add(new Marque(4893353, "False", 1246974, "", 4, "UV", "NISSAN", "", "Y"));
        myTableRows.add(new Marque(4893557, "False", 1246974, "", 2, "UV", "SUBARU", "", "Y"));
        myTableRows.add(new Marque(4894018, "False", 1246974, "", 2, "UV", "MAZDA", "", "Y"));
        myTableRows.add(new Marque(4895203, "False", 1246974, "", 2, "UV", "ROVER", "", "Y"));
        myTableRows.add(new Marque(4896300, "False", 1246974, "", 2, "UV", "HYUNDAI", "", "Y"));
        myTableRows.add(new Marque(4899527, "False", 1246974, "", 1, "UV", "JEEP", "", "Y"));
        myTableRows.add(new Marque(7659280, "False", 1246972, "Y", 3, "", "DAIHATSU", "", "Y"));
        myTableRows.add(new Marque(7681544, "False", 1246974, "", 2, "UV", "LANDROVER", "", "Y"));
        myTableRows.add(new Marque(7730022, "False", 1246974, "", 2, "UV", "VOLVO", "", "Y"));
        myTableRows.add(new Marque(8376505, "False", 1246974, "", 0, "", "ISUZU", "", "Y"));
        myTableRows.add(new Marque(8587147, "False", 1246974, "", 0, "", "DAEWOO", "", "Y"));
        myTableRows.add(new Marque(9971178, "False", 1246974, "", 1, "", "CHRYSLER", "", "Y"));
        myTableRows.add(new Marque(13224369, "False", 1246974, "", 0, "", "VW", "", "Y"));
        myTableRows.add(new Marque(6664478, "False", 1246974, "", 0, "", "BMW", "", "Y"));
        myTableRows.add(new Marque(1, "False", 1246974, "", 0, "", "TOYOTA", "", "Y"));
        myTableRows.add(new Marque(2, "False", 1246974, "", 0, "", "HUMMER", "", "Y"));

        marques.insert(myTableRows);
        DBTable.setPrintSQLBeforeExecuting(true);

    }

    @Override
    protected void tearDown() throws Exception {
        myDatabase.dropTable(myTableRow);

        super.tearDown();
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
