package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;

import org.junit.Test;

@SuppressWarnings("serial")
public class PropertyWrapperTest {
	@Test
	public void dotEqualsFalseWhenSameFieldOnSameObjectButRetrievedSeparately() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField1 = new DBInteger();

			@DBColumn
			public DBInteger intField2 = new DBInteger();
		}
		
		MyClass obj = new MyClass();
		PropertyWrapper intField1_obj1 = propertyOf(obj, "intField1");
		PropertyWrapper intField1_obj2 = propertyOf(obj, "intField1");
		
		assertThat(intField1_obj1 == intField1_obj2, is(false));
		assertThat(intField1_obj1.equals(intField1_obj2), is(true));
	}
	
	@Test
	public void dotEqualsFalseWhenSameFieldOnDifferentObject() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField1 = new DBInteger();

			@DBColumn
			public DBInteger intField2 = new DBInteger();
		}
		
		PropertyWrapper intField1_obj1 = propertyOf(new MyClass(), "intField1");
		PropertyWrapper intField1_obj2 = propertyOf(new MyClass(), "intField1");
		assertThat(intField1_obj1 == intField1_obj2, is(false));
		
		assertThat(intField1_obj1.equals(intField1_obj2), is(false));
	}

	@Test
	public void dotEqualsTrueWhenDifferentButIdenticalClass() {
		class MyClass1 extends DBRow {
			@DBColumn
			public DBInteger intField1 = new DBInteger();

			@DBColumn
			public DBInteger intField2 = new DBInteger();
		}

		class MyClass2 extends DBRow {
			@DBColumn
			public DBInteger intField1 = new DBInteger();

			@DBColumn
			public DBInteger intField2 = new DBInteger();
		}
		
		PropertyWrapper intField1_obj1 = propertyOf(new MyClass1(), "intField1");
		PropertyWrapper intField1_obj2 = propertyOf(new MyClass2(), "intField1");
		assertThat(intField1_obj1.equals(intField1_obj2), is(false));
	}
	
	@Test
	public void getsTableNameViaProperty() {
		@DBTableName("Customer")
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField1 = new DBInteger();
		}
		
		PropertyWrapper property = propertyOf(new MyClass(), "intField1");
		assertThat(property.tableName(), is("Customer"));
	}
	
	// note: intentionally doesn't use a wrapper factory for tests on equals() methods
	private PropertyWrapper propertyOf(Object target, String javaPropertyName) {
		DBRowClassWrapper classWrapper = new DBRowClassWrapper(target.getClass());
		return classWrapper.instanceAdaptorFor(target).getPropertyByName(javaPropertyName);
	}
}
