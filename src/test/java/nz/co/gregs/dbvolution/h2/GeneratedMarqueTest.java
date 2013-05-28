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
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBTableRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.generation.DBSchema;
import nz.co.gregs.dbvolution.generation.Marque;

/**
 *
 * @author gregorygraham
 */
public class GeneratedMarqueTest extends TestCase {

    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolution", "", "");
    nz.co.gregs.dbvolution.example.Marque myTableRow = new nz.co.gregs.dbvolution.example.Marque();
    DBTable<Marque> marques;

    public GeneratedMarqueTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        myDatabase.dropTableNoExceptions(myTableRow);
        myDatabase.createTable(myTableRow);

        DBTable.setPrintSQLBeforeExecuting(false);
        DBTable<nz.co.gregs.dbvolution.example.Marque> originalMarques = new DBTable<nz.co.gregs.dbvolution.example.Marque>(myTableRow, myDatabase);

        List<nz.co.gregs.dbvolution.example.Marque> myTableRows = new ArrayList<nz.co.gregs.dbvolution.example.Marque>();
        myTableRows.add(new nz.co.gregs.dbvolution.example.Marque(4893059, "False", 1246974, "", 3, "UV", "PEUGEOT", "", "Y"));
        myTableRows.add(new nz.co.gregs.dbvolution.example.Marque(4893090, "False", 1246974, "", 1, "UV", "FORD", "", "Y"));
        myTableRows.add(new nz.co.gregs.dbvolution.example.Marque(4893101, "False", 1246974, "", 2, "UV", "HOLDEN", "", "Y"));

        originalMarques.insert(myTableRows);
        DBTable.setPrintSQLBeforeExecuting(true);
    }

    @Override
    protected void tearDown() throws Exception {
        myDatabase.dropTable(myTableRow);

        super.tearDown();
    }
    // TODO add test methods here. The name must begin with 'test'. For example:
    // public void testHello() {}

    public void testGetSchema() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        DBSchema.generateSchema(myDatabase, "nz.co.gregs.dbvolution.generation");
    }

    public void testGetAllRows() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        marques = new DBTable<Marque>(new Marque(), myDatabase);
        marques.getAllRows();
        for (DBTableRow row : marques) {
            System.out.println(row);
        }
    }

    public void testGetFirstAndPrimaryKey() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IntrospectionException, InstantiationException, SQLException, ClassNotFoundException {
        marques = new DBTable<Marque>(new Marque(), myDatabase);
        DBTableRow row = marques.firstRow();
        if (row != null) {
            String primaryKey = row.getPrimaryKey();
            DBTable<Marque> singleMarque = new DBTable<Marque>(new Marque(), myDatabase);
            singleMarque.getByPrimaryKey(primaryKey).printAllRows();
        }
    }

    public void testRawQuery() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        marques = new DBTable<Marque>(new Marque(), myDatabase);
        String rawQuery = "and lower(name) in ('toyota','hummer') ;  ";
        marques = marques.getByRawSQL(rawQuery);
        marques.printAllRows();

    }
}
