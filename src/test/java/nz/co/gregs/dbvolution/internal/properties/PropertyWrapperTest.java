package nz.co.gregs.dbvolution.internal.properties;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBJavaObject;
import nz.co.gregs.dbvolution.datatypes.DBLargeText;
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
		var intField1_obj1 = propertyOf(obj, "intField1");
		var intField1_obj2 = propertyOf(obj, "intField1");

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

		var intField1_obj1 = propertyOf(new MyClass(), "intField1");
		var intField1_obj2 = propertyOf(new MyClass(), "intField1");
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

		var intField1_obj1 = propertyOf(new MyClass1(), "intField1");
		var intField1_obj2 = propertyOf(new MyClass2(), "intField1");
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

		var intField1 = propertyOf(new MyClass1(), "intField1");
		var intField2 = propertyOf(new MyClass1(), "intField2");
		assertThat(intField1.hasColumnExpression(), is(false));
		assertThat(intField2.hasColumnExpression(), is(true));
	}

	@Test
	public void largeObjectTest() {
		class MyClass1 extends DBRow {

			@DBColumn
			public DBInteger intField1 = new DBInteger();

			@DBColumn
			public DBJavaObject<String> javaObject = new DBJavaObject<String>();
			@DBColumn
			public DBLargeText largeText = new DBLargeText();
		}

		var intField1 = propertyOf(new MyClass1(), "intField1");
		var javaField = propertyOf(new MyClass1(), "javaObject");
		var textField = propertyOf(new MyClass1(), "largeText");
		assertThat(intField1.isLargeObjectType(), is(false));
		assertThat(javaField.isLargeObjectType(), is(true));
		assertThat(textField.isLargeObjectType(), is(true));
	}

	// note: intentionally doesn't use a wrapper factory for tests on equals() methods
	@SuppressWarnings("unchecked")
	private <ROW extends DBRow> PropertyWrapper<?, ?, ?> propertyOf(ROW target, String javaPropertyName) {
		var classWrapper = new RowDefinitionClassWrapper<ROW>((Class<ROW>) target.getClass());
		return classWrapper.instanceWrapperFor(target).getPropertyByName(javaPropertyName);
	}
}
