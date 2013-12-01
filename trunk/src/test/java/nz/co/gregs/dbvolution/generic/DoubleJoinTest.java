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
package nz.co.gregs.dbvolution.generic;

import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.example.CarCompany;
import org.junit.Ignore;
import org.junit.Test;

public class DoubleJoinTest extends AbstractTest {

    public DoubleJoinTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void fake() throws SQLException {}
    
    @Ignore // not working yet
    @Test
    public void doubleJoinTest() throws SQLException {
        database.createTable(new DoubleJoinTest.DoubleLinked());
        final DoubleLinked doubleLinked = new DoubleJoinTest.DoubleLinked();
        doubleLinked.uidDoubleLink.setValue(1);
        doubleLinked.manufacturer.setValue(1);
        doubleLinked.marketer.setValue(4);
        database.insert(doubleLinked);
        DBQuery query = database.getDBQuery(new DoubleJoinTest.DoubleLinked(), new DoubleJoinTest.Manufacturer(), new DoubleJoinTest.Marketer());
        query.setBlankQueryAllowed(true);
        List<DBQueryRow> allRows = query.getAllRows();

        database.print(allRows);
    }

    @DBTableName("double_linked")
    public static class DoubleLinked extends DBRow {

        @DBPrimaryKey()
        @DBColumn("uid_doublellink")
        DBInteger uidDoubleLink = new DBInteger();
        @DBForeignKey(CarCompany.class)
        @DBColumn("fkmanufacturer")
        DBInteger manufacturer = new DBInteger();
        @DBForeignKey(CarCompany.class)
        @DBColumn("fkmarketer")
        DBInteger marketer = new DBInteger();

        public DoubleLinked() {
            super();
        }
    }

    public static class Manufacturer extends CarCompany {

        public Manufacturer() {
            super();
        }
    }

    public static class Marketer extends CarCompany {

        public Marketer() {
            super();
        }
    }
}
