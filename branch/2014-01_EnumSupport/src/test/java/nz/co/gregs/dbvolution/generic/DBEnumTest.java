/*
 * Copyright 2014 gregory.graham.
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

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBIntegerEnum;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBStringEnum;
import nz.co.gregs.dbvolution.generic.DBEnumTest.SomeTable.RecordType;
import nz.co.gregs.dbvolution.generic.DBEnumTest.OtherTable.StringEnumType;

import org.junit.Test;

public class DBEnumTest extends AbstractTest {

    public DBEnumTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void createRecord() {
        SomeTable row = new SomeTable();
        row.recordType.setValue(RecordType.MOVEMENT_CANCELLATION_REQUEST); // nested class imported
        row.recordType.setValue(SomeTable.RecordType.MOVEMENT_CANCELLATION_REQUEST); // explicit reference to nested class
    }

    @Test
    public void filterRecord() {
        SomeTable rowExemplar = new SomeTable();
        rowExemplar.recordType.permittedValues(RecordType.MOVEMENT_REQUEST_RECORD, RecordType.SHIPPING_MANIFEST_RECORD);
    }

    @Test
    public void processIntegerRecord() throws SQLException {
        final SomeTable someTable = new SomeTable();
        database.createTable(someTable);
        database.insert(
                new SomeTable(1, RecordType.MOVEMENT_REQUEST_RECORD),
                new SomeTable(2, RecordType.SHIPPING_MANIFEST_RECORD),
                new SomeTable(4, RecordType.MOVEMENT_REQUEST_RECORD));

        someTable.recordType.permittedValues(
                RecordType.MOVEMENT_CANCELLATION_REQUEST.getLiteralValue(),
                RecordType.MOVEMENT_REQUEST_RECORD.getLiteralValue(),
                RecordType.SHIPPING_MANIFEST_RECORD.getLiteralValue());
        List<SomeTable> rows = database.get(someTable);
        database.print(rows);
        database.dropTable(someTable);
    }

    @Test
    public void processStringRecord() throws SQLException {
        final OtherTable otherTable = new OtherTable();
        database.createTable(otherTable);
        database.insert(
                new OtherTable(1, StringEnumType.MOVEMENT_REQUEST_RECORD),
                new OtherTable(2, StringEnumType.SHIPPING_MANIFEST_RECORD),
                new OtherTable(4, StringEnumType.MOVEMENT_REQUEST_RECORD));

        otherTable.recordType.permittedValues(
                StringEnumType.MOVEMENT_CANCELLATION_REQUEST.getLiteralValue(),
                StringEnumType.MOVEMENT_REQUEST_RECORD.getLiteralValue(),
                StringEnumType.SHIPPING_MANIFEST_RECORD.getLiteralValue());
        List<OtherTable> rows = database.get(otherTable);
        database.print(rows);
        database.dropTable(otherTable);
    }

    @Test
    public void displayInJsp() {
        /*
         jsp: <%-- in a table --%>
         <table>		
         <c:forEach var="records" item="record">
         <tr>
         <td>${record.recordType.displayName}</td>
         </tr>
         </c:forEach>
         </table>
		   
         <%-- in a selection dropdown --%>
         <form:select path="recordType">
         <form:options items="${allRecordTypes}" itemLabel="displayName"/>
         </form:select>
         */
    }

    public static class SomeTable extends DBRow {

        private static final long serialVersionUID = 1L;

        public SomeTable() {
        }

        public SomeTable(Integer uid, RecordType recType) {
            this.uid_202.setValue(uid);
            this.recordType.setValue(recType);
        }
        @DBColumn("uid_202")
        @DBPrimaryKey
        public DBInteger uid_202 = new DBInteger();
        
        @DBColumn("c_5")
        public DBIntegerEnum<RecordType> recordType = new DBIntegerEnum<RecordType>();

        /**
         * Valid values for {@link #recordType}
         */
        // Nested class to make it obvious which table the enum is for
        public static enum RecordType implements DBEnumValue<Integer> {

            SHIPPING_MANIFEST_RECORD(1, "Shipping Manifest Record"),
            MOVEMENT_REQUEST_RECORD(2, "Movement Request Record"),
            MOVEMENT_CANCELLATION_REQUEST(3, "Movement Cancellation Request");
            private int literalValue;
            private String displayName;

            private RecordType(int code, String displayName) {
                this.literalValue = code;
                this.displayName = displayName;
            }

            @Override
            public Integer getLiteralValue() {
                return literalValue;
            }

            public String getDisplayName() {
                return displayName;
            }

            public static RecordType valueOfCode(DBInteger code) {
                return valueOfCode(code == null ? null : code.intValue());
            }

            public static RecordType valueOfCode(Integer code) {
                if (code == null) {
                    return null;
                }
                for (RecordType recordType : values()) {
                    if (recordType.getLiteralValue() == code) {
                        return recordType;
                    }
                }
                throw new IllegalArgumentException("Invalid " + RecordType.class.getSimpleName() + " code: " + code);
            }
        }
    }

    public static class OtherTable extends DBRow {

        private static final long serialVersionUID = 1L;

        public OtherTable() {
        }

        public OtherTable(Integer uid, StringEnumType recType) {
            this.uid_202.setValue(uid);
            this.recordType.setValue(recType);
        }
        @DBColumn("uid_203")
        @DBPrimaryKey
        public DBInteger uid_202 = new DBInteger();
        
        @DBColumn("c_5")
        public DBStringEnum<StringEnumType> recordType = new DBStringEnum<StringEnumType>();

        /**
         * Valid values for {@link #recordType}
         */
        // Nested class to make it obvious which table the enum is for
        public static enum StringEnumType implements DBEnumValue<String> {

            SHIPPING_MANIFEST_RECORD("MANRECORD", "Shipping Manifest Record"),
            MOVEMENT_REQUEST_RECORD("MOVEREQ", "Movement Request Record"),
            MOVEMENT_CANCELLATION_REQUEST("CANCREQ", "Movement Cancellation Request");
            private String literalValue;
            private String displayName;

            private StringEnumType(String code, String displayName) {
                this.literalValue = code;
                this.displayName = displayName;
            }

            @Override
            public String getLiteralValue() {
                return literalValue;
            }

            public String getDisplayName() {
                return displayName;
            }

            public static StringEnumType valueOfCode(DBString code) {
                return valueOfCode(code == null ? null : code.stringValue());
            }

            public static StringEnumType valueOfCode(String code) {
                if (code == null) {
                    return null;
                }
                for (StringEnumType recordType : values()) {
                    if (recordType.getLiteralValue().equals(code)) {
                        return recordType;
                    }
                }
                throw new IllegalArgumentException("Invalid " + StringEnumType.class.getSimpleName() + " code: " + code);
            }
        }
    }
}
