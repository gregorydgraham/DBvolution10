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
import nz.co.gregs.dbvolution.annotations.DBEnumType;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBEnum;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.generic.DBEnumTest.SomeTable.RecordType;

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
	public void processRecord() throws SQLException {
		List<SomeTable> rows = database.get(new SomeTable());
		for (SomeTable row: rows) {
			if (!row.recordType.isNull()) {
				// handle any
				switch (row.recordType.enumValue()) {
					case MOVEMENT_CANCELLATION_REQUEST: return;
					case MOVEMENT_REQUEST_RECORD: return;
					case SHIPPING_MANIFEST_RECORD: return;
				}
				
				// handle cancellations
				if (row.recordType.enumValue() == RecordType.MOVEMENT_CANCELLATION_REQUEST) {
					return;
				}
			}
		}
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
	    @DBColumn("uid_202")
	    @DBPrimaryKey
	    public DBInteger uid_202 = new DBInteger();

	    // option 1: requires TypeAdaptor, doesn't require DBEnumValue interface
//	    @DBColumn("c_5")
//	    @DBAdaptType(RecordTypeFromCodeAdaptor.class)
//	    public DBEnum<RecordType> recordType = new DBEnum<RecordType>();

	    // option 2: requires new annotation and DBEnumValue interface
	    // (may also require separate DBIntegerEnum and DBStringEnum)
	    @DBColumn("c_5")
	    @DBEnumType(RecordType.class)
	    public DBEnum<RecordType> recordType = new DBEnum<RecordType>();
	    
	    // option 3: passed into constructor
	    // (won't work, because DBvolution doesn't know what to do if the qdt reference is null)
//	    @DBColumn("c_5")
//	    public DBEnum<RecordType> recordType3 = new DBEnum<RecordType>(RecordType.class);
	    
	    // option 4: build enum support into basic QDT types
//	    @DBColumn("c_5")
//	    public DBInteger recordType4 = new DBInteger();
//	    {
//	    	RecordType type = recordType4.getEnumValue(RecordType.class);
//	    	recordType4.setEnumValue(RecordType.SHIPPING_MANIFEST_RECORD);
//	    }
	    
	    // required for option (2)
//	    public static class RecordTypeFromCodeAdaptor implements DBTypeAdaptor<RecordType, Integer> {
//			@Override
//			public RecordType fromDatabaseValue(Integer dbvValue) {
//				return (dbvValue == null) ? null : RecordType.valueOfCode(dbvValue);
//			}
//
//			@Override
//			public Integer toDatabaseValue(RecordType objectValue) {
//				return (objectValue == null) ? null : objectValue.getCode();
//			}
//	    }
	    
	    /** Valid values for {@link #recordType} */
	    // Nested class to make it obvious which table the enum is for
		public static enum RecordType implements DBEnumValue {
			SHIPPING_MANIFEST_RECORD(1,"Shipping Manifest Record"),
			MOVEMENT_REQUEST_RECORD(2,"Movement Request Record"),
			MOVEMENT_CANCELLATION_REQUEST(3,"Movement Cancellation Request");
			
			private int code;
			private String displayName;
			
			private RecordType(int code, String displayName) {
				this.code = code;
				this.displayName = displayName;
			}
			
			public Integer getCode() {
				return code;
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
				for (RecordType recordType: values()) {
					if (recordType.getCode() == code) {
						return recordType;
					}
				}
				throw new IllegalArgumentException("Invalid "+RecordType.class.getSimpleName()+" code: "+code);
			}
		}
	}

}
