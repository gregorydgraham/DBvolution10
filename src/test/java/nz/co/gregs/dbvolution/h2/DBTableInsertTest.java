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
public class DBTableInsertTest extends AbstractTest {
    
//    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolutionInsertTest", "", "");
    Marque myTableRow = new Marque();
//    DBTable<Marque> marques;
    
    public DBTableInsertTest(String testName) {
        super(testName);
    }
    
//    @Override
//    protected void setUp() throws Exception {
//        super.setUp();
//        myDatabase.dropTableNoExceptions(myTableRow);
//        myDatabase.createTable(myTableRow);
//        DBTable.setPrintSQLBeforeExecuting(true);
//        marques = new DBTable<Marque>(myTableRow, myDatabase);
//    }
//    
//    @Override
//    protected void tearDown() throws Exception {
//        myDatabase.dropTable(myTableRow);
//        
//        super.tearDown();
//    }
    // TODO add test methods here. The name must begin with 'test'. For example:
    // public void testHello() {}

    public void testInsertRows() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        myTableRow.getUidMarque().isLiterally(999);
        myTableRow.getName().isLiterally("TOYOTA");
        myTableRow.getNumericCode().isLiterally(10);
        marques.insert(myTableRow);
        marques.getAllRows();
        marques.printAllRows();
        
        List<Marque> myTableRows = new ArrayList<Marque>();
        myTableRows.add(new Marque(3, "False", 1246974, "", 3, "UV", "TVR", "", "Y",4));
        
        marques.insert(myTableRows);
        marques.size();
        marques.getAllRows();
        marques.printAllRows();
    }
}
