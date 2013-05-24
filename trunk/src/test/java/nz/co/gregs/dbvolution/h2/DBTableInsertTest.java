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
public class DBTableInsertTest extends TestCase {

    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolution", "", "");
    Marque myTableRow = new Marque();
    DBTable<Marque> marques;

    public DBTableInsertTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        myDatabase.createTable(myTableRow);
        DBTable.setPrintSQLBeforeExecuting(true);
        marques = new DBTable<Marque>(myTableRow, myDatabase);
    }

    @Override
    protected void tearDown() throws Exception {
        myDatabase.dropTable(myTableRow);

        super.tearDown();
    }
    // TODO add test methods here. The name must begin with 'test'. For example:
    // public void testHello() {}

    public void testInsertRows() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        myTableRow.getUidMarque().isLiterally(2);
        myTableRow.getName().isLiterally("TOYOTA");
        myTableRow.getNumericCode().isLiterally(10);
        marques.insert(myTableRow);
        marques.getAllRows();
        marques.printAllRows();

        List<Marque> myTableRows = new ArrayList<Marque>();
        myTableRows.add(new Marque(4893059, "False", 1246974, "", 3, "UV", "PEUGEOT", "", "Y"));
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

        marques.insert(myTableRows);
        marques.size();
        marques.getAllRows();
        marques.printAllRows();
    }
}
