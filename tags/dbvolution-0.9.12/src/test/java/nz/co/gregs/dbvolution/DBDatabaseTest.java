/*
 * Copyright 2013 Gregory Graham.
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
package nz.co.gregs.dbvolution;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.H2DB;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalDroppingOfTableException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Gregory Graham
 */
public class DBDatabaseTest {

    DBDatabase myDatabase = new H2DB("jdbc:h2:mem:dbvolutionTest", "", "");

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreateTable() throws SQLException {

        try {
            myDatabase.dropTable(new Marque());
        } catch (Exception ex) {
            System.out.println("SETUP: Marque table not dropped, probably doesn't exist");

        }
        myDatabase.createTable(new Marque());
        System.out.println("Marque table created successfully");

    }

    @Test
    public void testDropTableException() throws SQLException {
        try {
            myDatabase.createTable(new Marque());
        } catch (Exception ex) {
            System.out.println("SETUP: Marque table not created, probably already exists");
        }
        try{
        myDatabase.dropTable(new Marque());
        myDatabase.preventDroppingOfTables(false);
        myDatabase.dropTable(new Marque());
        throw new DBRuntimeException("Drop Table Method failed to throw a AccidentalDroppingOfTableException exception.");
        }catch(AccidentalDroppingOfTableException oops){
            System.out.println("AccidentalDroppingOfTableException successfully thrown");
        }
    }

    @Test
    public void testDropTable() throws SQLException {
        try {
            myDatabase.createTable(new Marque());
        } catch (Exception ex) {
            System.out.println("SETUP: Marque table not created, probably already exists");
        }
        myDatabase.preventDroppingOfTables(false);
        myDatabase.dropTable(new Marque());
        System.out.println("Marque table dropped successfully");

    }
}
