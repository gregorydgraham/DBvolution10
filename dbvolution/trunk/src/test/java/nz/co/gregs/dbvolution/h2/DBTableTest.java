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
import java.util.Date;
import junit.framework.TestCase;
import nz.co.gregs.dbvolution.DBTable;
import nz.co.gregs.dbvolution.DBTableRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.example.Marque;

/**
 *
 * @author gregory.graham
 */
public class DBTableTest extends TestCase {

    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolution", "", "");
    Marque myTableRow = new Marque();
    DBTable<Marque> marques;

    public DBTableTest(String testName) {
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
        super.tearDown();
        myDatabase.dropTable(myTableRow);
    }
    // TODO add test methods here. The name must begin with 'test'. For example:
    // public void testHello() {}

    public void testGetAllRows() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        marques.getAllRows();
        for (DBTableRow row : marques) {
            System.out.println(row);
        }
    }

    public void testGetFirstAndPrimaryKey() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IntrospectionException, InstantiationException, SQLException, ClassNotFoundException {
        DBTableRow row = marques.firstRow();
        if (row != null) {
            String primaryKey = row.getPrimaryKey();
            DBTable<Marque> singleMarque = new DBTable<Marque>(new Marque(), myDatabase);
            singleMarque.getByPrimaryKey(primaryKey).printAllRows();
        }
    }
    
    public void testisBetween() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException{
                Marque marqueQuery = new Marque();
        marqueQuery.getName().isLike("%T%");
        marqueQuery.getNumericCode().isBetween(0, 90000000);
        //System.out.println(marques.getSQLForExample(marqueQuery));
        marques = marques.getByExample(marqueQuery);
        for (Marque row : marques) {
            System.out.println(row);
        }
    }
    public void testIsLiterally() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException{
        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().isLiterally(1L);
        marques = marques.getByExample(hummerQuery);
        marques.printAllRows();
    }
    public void testIsIn() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException{
        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().blankQuery();
        hummerQuery.getName().isIn(new String[]{"TOYOTA", "HUMMER"});
        marques = marques.getByExample(hummerQuery);
        marques.printAllRows();}

    public void testDateIsBetween() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException{
        Marque oldQuery = new Marque();
        oldQuery.getCreationDate().isBetween(new Date(0L), new Date());
        marques = marques.getByExample(oldQuery);
        marques.printAllRows();
    }
    public void testRawQuery() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException{
        String rawQuery = "and lower(name) in ('toyota','hummer') ;  ";
        marques = marques.getByRawSQL(rawQuery);
        marques.printAllRows();

    }
}
