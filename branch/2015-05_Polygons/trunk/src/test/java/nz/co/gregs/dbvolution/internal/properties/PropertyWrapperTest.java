package nz.co.gregs.dbvolution.internal.properties;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.expressions.NumberExpression;

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
	public void columnExpressionTest() {
		class MyClass1 extends DBRow {

			@DBColumn
			public DBInteger intField1 = new DBInteger();

			@DBColumn
			public DBInteger intField2 = new DBInteger(NumberExpression.countAll());
		}

		PropertyWrapper intField1 = propertyOf(new MyClass1(), "intField1");
		PropertyWrapper intField2 = propertyOf(new MyClass1(), "intField2");
		assertThat(intField1.hasColumnExpression(), is(false));
		assertThat(intField2.hasColumnExpression(), is(true));
	}

	// note: intentionally doesn't use a wrapper factory for tests on equals() methods
	private PropertyWrapper propertyOf(DBRow target, String javaPropertyName) {
		RowDefinitionClassWrapper classWrapper = new RowDefinitionClassWrapper(target.getClass());
		return classWrapper.instanceWrapperFor(target).getPropertyByName(javaPropertyName);
	}
}
