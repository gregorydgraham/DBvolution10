package nz.co.gregs.dbvolution.internal;

import java.util.Date;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Focuses on ensuring that at a high level these different scenarios
 * are supported by the library. 
 */
@SuppressWarnings("serial")
public class TypeAdaptorUsabilityTest {
	@Test
	public void integerFieldAdaptedAsDBInteger_whenAdaptingOnSimpleTypes() {
		class MyTypeAdaptor implements DBTypeAdaptor<Integer, Long> {
			public Integer fromDatabaseValue(Long dbvValue) {
				return (dbvValue == null) ? null : dbvValue.intValue();
			}

			public Long toDatabaseValue(Integer objectValue) {
				return (objectValue == null) ? null : objectValue.longValue();
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBInteger.class)
			public Integer year;
		}
	}

	@Test
	public void stringFieldAdaptedAsDBInteger_whenAdaptingOnSimpleTypes() {
		class MyTypeAdaptor implements DBTypeAdaptor<String, Long> {
			public String fromDatabaseValue(Long dbvValue) {
				return (dbvValue == null) ? null : dbvValue.toString();
			}

			public Long toDatabaseValue(String objectValue) {
				return (objectValue == null) ? null : Long.parseLong(objectValue);
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBInteger.class)
			public String year;
		}
	}
	
	@Test
	public void dbstringFieldAdaptedAsDBInteger_whenAdaptingOnSimpleTypes() {
		class MyTypeAdaptor implements DBTypeAdaptor<String, Long> {
			public String fromDatabaseValue(Long dbvValue) {
				return (dbvValue == null) ? null : dbvValue.toString();
			}

			public Long toDatabaseValue(String objectValue) {
				return (objectValue == null) ? null : Long.parseLong(objectValue);
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBInteger.class)
			public DBString year;
		}
	}
	
	@Test
	@SuppressWarnings("deprecation")
	public void integerFieldAdaptedAsDBDate_whenAdaptingOnSimpleTypes() {
		class MyTypeAdaptor implements DBTypeAdaptor<Integer, Date> {
			public Integer fromDatabaseValue(Date dbvValue) {
				return (dbvValue == null) ? null : dbvValue.getYear()+1900;
			}

			public Date toDatabaseValue(Integer objectValue) {
				return (objectValue == null) ? null : new Date(objectValue-1900, 0, 1);
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBDate.class)
			public Integer year;
		}
	}
	
	@Test
	@SuppressWarnings("deprecation")
	public void dbintegerFieldAdaptedAsDBDate_whenAdaptingOnSimpleTypes() {
		class MyTypeAdaptor implements DBTypeAdaptor<Long, Date> {
			public Long fromDatabaseValue(Date dbvValue) {
				return (dbvValue == null) ? null : (long)(dbvValue.getYear()+1900);
			}

			public Date toDatabaseValue(Long objectValue) {
				return (objectValue == null) ? null : new Date(objectValue.intValue()-1900, 0, 1);
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBDate.class)
			public DBInteger year;
		}
	}

	@Test
	@SuppressWarnings("deprecation")
	public void dateFieldAdaptedAsDBInteger_whenAdaptingOnSimpleTypes() {
		class MyTypeAdaptor implements DBTypeAdaptor<Date, Integer> {
			public Date fromDatabaseValue(Integer dbvValue) {
				return (dbvValue == null) ? null : new Date(dbvValue-1900, 0, 1);
			}

			public Integer toDatabaseValue(Date objectValue) {
				return (objectValue == null) ? null : objectValue.getYear()+1900;
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBInteger.class)
			public Date year;
		}
	}

	@Test
	@SuppressWarnings("deprecation")
	public void dbdateFieldAdaptedAsDBInteger_whenAdaptingOnSimpleTypes() {
		class MyTypeAdaptor implements DBTypeAdaptor<Date, Integer> {
			public Date fromDatabaseValue(Integer dbvValue) {
				return (dbvValue == null) ? null : new Date(dbvValue-1900, 0, 1);
			}

			public Integer toDatabaseValue(Date objectValue) {
				return (objectValue == null) ? null : objectValue.getYear()+1900;
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBInteger.class)
			public DBDate year;
		}
	}
	
	@Test
	public void complexFieldAdaptedAsDBString_whenAdaptingOnComplexPOJOTypes() {
		class MyDataType {
			public MyDataType parse(String str) {
				return new MyDataType();
			}
			
			@Override
			public String toString() {
				return MyDataType.class.getSimpleName();
			}
		}
		
		class MyTypeAdaptor implements DBTypeAdaptor<MyDataType, String> {
			public MyDataType fromDatabaseValue(String dbvValue) {
				return (dbvValue == null) ? null : new MyDataType().parse(dbvValue);
			}

			public String toDatabaseValue(MyDataType objectValue) {
				return (objectValue == null) ? null : objectValue.toString();
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBString.class)
			public MyDataType obj;
		}
	}

	// not sure if need to support this
	@Test
	public void stringFieldAdaptedAsCustomQDT_whenAdaptingOnSimpleTypes() {
		class MyQDT extends QueryableDatatype {
			@SuppressWarnings("unused")
			public MyQDT() {
				super(new Integer(23));
			}
			
			@Override
			public String getSQLDatatype() {
				return "unknown";
			}

			@Override
			protected String formatValueForSQLStatement(DBDatabase db) {
				return "unknown";
			}
		}
		
		class MyTypeAdaptor implements DBTypeAdaptor<String, Integer> {
			public String fromDatabaseValue(Integer dbvValue) {
				return (dbvValue == null) ? null : dbvValue.toString();
			}

			public Integer toDatabaseValue(String objectValue) {
				return (objectValue == null) ? null : Integer.parseInt(objectValue);
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=MyQDT.class)
			public String year;
		}
	}
	
	// not trying to support just yet
	@Ignore
	@Test
	public void stringFieldAdaptedAsCustomQDT_whenAdaptingOnComplexPOJOTypes() {
		class MyDataType {
			public MyDataType parse(String str) {
				return new MyDataType();
			}
			
			@Override
			public String toString() {
				return MyDataType.class.getSimpleName();
			}
		}

		class MyQDT extends QueryableDatatype {
			@SuppressWarnings("unused")
			public MyQDT() {
				super(new MyDataType());
			}
			
			@Override
			public String getSQLDatatype() {
				return "unknown";
			}

			@Override
			protected String formatValueForSQLStatement(DBDatabase db) {
				return "unknown";
			}
		}
		
		class MyTypeAdaptor implements DBTypeAdaptor<String, MyDataType> {
			public String fromDatabaseValue(MyDataType dbvValue) {
				return (dbvValue == null) ? null : dbvValue.toString();
			}

			public MyDataType toDatabaseValue(String objectValue) {
				return (objectValue == null) ? null : new MyDataType().parse(objectValue);
			}
		}
		
		@DBTableName("Customer")
		class MyTable extends DBRow {
			@DBColumn
			@DBAdaptType(adaptor=MyTypeAdaptor.class, type=MyQDT.class)
			public String text;
		}
	}
	
// --- ideas not supported yet ---
//	@Test
//	public static void integerFieldAdaptedAsDBInteger_withImplicitTypeAdaptor() {
//		@DBTableName("Customer")
//		class MyTable extends DBRow {
//			@DBColumn
//			@DBAdaptType(type=DBInteger.class)
//			public Integer year;
//		}
//	}
}
