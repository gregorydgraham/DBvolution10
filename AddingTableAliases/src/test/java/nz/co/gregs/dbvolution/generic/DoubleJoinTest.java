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
import static org.hamcrest.Matchers.is;
import org.junit.*;

public class DoubleJoinTest extends AbstractTest {

    public DoubleJoinTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void fake() throws SQLException {}
    
//    @Ignore // not working yet
    @Test
    public void doubleJoinTest() throws SQLException {
        database.setPrintSQLBeforeExecuting(true);
        database.createTable(new DoubleJoinTest.DoubleLinked());
        final DoubleLinked doubleLinked = new DoubleJoinTest.DoubleLinked();
        doubleLinked.uidDoubleLink.setValue(1);
        doubleLinked.manufacturer.setValue(1);
        doubleLinked.marketer.setValue(4);
        database.insert(doubleLinked);
        final DoubleLinked doubleLinked1 = new DoubleJoinTest.DoubleLinked();
        final Manufacturer manufacturer = new DoubleJoinTest.Manufacturer();
        manufacturer.setTableAlias("manufacturer");
        final Marketer marketer = new DoubleJoinTest.Marketer();
        marketer.setTableAlias("marketer");
        DBQuery query = database.getDBQuery(doubleLinked1, manufacturer, marketer);
        query.setBlankQueryAllowed(true);
        System.out.println(query.getSQLForQuery());
        List<DBQueryRow> allRows = query.getAllRows();
        Assert.assertThat(allRows.size(), 
                is(1));
        Assert.assertThat(allRows.get(0).get(marketer).uidCarCompany.intValue(), 
                is(doubleLinked.marketer.intValue()));
        Assert.assertThat(allRows.get(0).get(manufacturer).uidCarCompany.intValue(), 
                is(doubleLinked.manufacturer.intValue()));
                /*
        
 SELECT _1000112490.UID_DOUBLELLINK DB1343557695, 
_1000112490.FKMANUFACTURER DB136664381, 
_1000112490.FKMARKETER DB_354660011, 
__1805027343.NAME DB1064314813,            ***** THIS IS THE SAME AS THE COLUMN BELOW
__1805027343.UID_CARCOMPANY DB819159114,   *** DITTO
_311617033.NAME DB1064314813,              ***** THIS IS THE SAME AS THE COLUMN BELOW
_311617033.UID_CARCOMPANY DB819159114      *** DITTO
 FROM  double_linked AS _1000112490  INNER JOIN car_company AS __1805027343  ON( 
_1000112490.FKMANUFACTURER = __1805027343.UID_CARCOMPANY )  INNER JOIN car_company AS _311617033  ON( 
_1000112490.FKMARKETER = _311617033.UID_CARCOMPANY ) 
 WHERE  1=1 

;
        */

        database.print(allRows);
    }

    @DBTableName("double_linked")
    public static class DoubleLinked extends DBRow {

        @DBPrimaryKey()
        @DBColumn("uid_doublellink")
        DBInteger uidDoubleLink = new DBInteger();
        @DBForeignKey(Manufacturer.class)
        @DBColumn("fkmanufacturer")
        DBInteger manufacturer = new DBInteger();
        @DBForeignKey(Marketer.class)
        @DBColumn("fkmarketer")
        DBInteger marketer = new DBInteger();

        public DoubleLinked() {
            super();
        }
    }

    @DBTableName("car_company")
    public static class Manufacturer extends CarCompany {
        public Manufacturer() {
            super();
        }
    }

    @DBTableName("car_company")
    public static class Marketer extends CarCompany {
        public Marketer() {
            super();
        }
    }
}
