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
import nz.co.gregs.dbvolution.example.FKBasedFKRecognisor;
import nz.co.gregs.dbvolution.example.UIDBasedPKRecognisor;
import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;
import nz.co.gregs.dbvolution.generation.DBTableClass;
import nz.co.gregs.dbvolution.generation.ForeignKeyRecognisor;
import nz.co.gregs.dbvolution.generation.Marque;
import nz.co.gregs.dbvolution.generation.PrimaryKeyRecognisor;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class GeneratedMarqueTest extends AbstractTest {

//    DBDatabase myDatabase = new H2DB("jdbc:h2:~/dbvolution", "", "");
    nz.co.gregs.dbvolution.example.Marque myTableRow = new nz.co.gregs.dbvolution.example.Marque();
//    DBTable<Marque> marques;

    public GeneratedMarqueTest(String testName) {
        super(testName);
    }

    // TODO add test methods here. The name must begin with 'test'. For example:
    // public void testHello() {}
    @Test
    public void testGetSchema() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        List<DBTableClass> generateSchema;
        generateSchema = DBTableClassGenerator.generateClassesOfTables(myDatabase, "nz.co.gregs.dbvolution.generation",new PrimaryKeyRecognisor(),new ForeignKeyRecognisor());
        for (DBTableClass dbcl : generateSchema) {
            System.out.print("" + dbcl.javaSource);
        }
    }

    @Test
    public void testGetSchemaWithRecognisor() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        List<DBTableClass> generateSchema;
        generateSchema = DBTableClassGenerator.generateClassesOfTables(myDatabase, "nz.co.gregs.dbvolution.generation",new UIDBasedPKRecognisor(),new FKBasedFKRecognisor());
        for (DBTableClass dbcl : generateSchema) {
            System.out.print("" + dbcl.javaSource);
        }
    }

    @Test
    public void testGetAllRows() throws IllegalArgumentException, IllegalAccessException, IntrospectionException, InvocationTargetException, SQLException, InstantiationException, NoSuchMethodException {
        DBTable<Marque> marq = new DBTable<Marque>(new Marque(), myDatabase);
        marq.getAllRows();
        for (DBTableRow row : marq) {
            System.out.println(row);
        }
    }

    @Test
    public void testGetFirstAndPrimaryKey() throws SQLException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IntrospectionException, InstantiationException, SQLException, ClassNotFoundException {
        DBTable<Marque> marq = new DBTable<Marque>(new Marque(), myDatabase);
        DBTableRow row = marq.firstRow();
        if (row != null) {
            String primaryKey = row.getPrimaryKeyValue();
            DBTable<Marque> singleMarque = new DBTable<Marque>(new Marque(), myDatabase);
            singleMarque.getByPrimaryKey(primaryKey).printAllRows();
        }
    }

    @Test
    public void testRawQuery() throws IllegalArgumentException, IllegalAccessException, SQLException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException, IntrospectionException {
        DBTable<Marque> marq = new DBTable<Marque>(new Marque(), myDatabase);
        String rawQuery = "and lower(name) in ('toyota','hummer') ;  ";
        marq = marq.getByRawSQL(rawQuery);
        marq.printAllRows();

    }
}
