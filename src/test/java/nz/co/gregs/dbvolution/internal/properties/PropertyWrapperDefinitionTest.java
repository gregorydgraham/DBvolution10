package nz.co.gregs.dbvolution.internal.properties;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.query.RowDefinition;

import org.junit.Test;

@SuppressWarnings("serial")
public class PropertyWrapperDefinitionTest {

	@Test
	public void dotEqualsTrueWhenDifferentObjectAndSameClass() {
		class MyClass extends DBRow {

			@DBColumn
			public DBInteger intField1 = new DBInteger();
			@DBColumn
			public DBInteger intField2 = new DBInteger();
		}

		var intField1_obj1 = propertyDefinitionOf(new MyClass(), "intField1");
		var intField1_obj2 = propertyDefinitionOf(new MyClass(), "intField1");
		assertThat(intField1_obj1 == intField1_obj2, is(false));

		assertThat(intField1_obj1.equals(intField1_obj2), is(true));
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

		var intField1_obj1 = propertyDefinitionOf(new MyClass1(), "intField1");
		var intField1_obj2 = propertyDefinitionOf(new MyClass2(), "intField1");
		assertThat(intField1_obj1.equals(intField1_obj2), is(false));
	}

	@Test
	public void getsTableNameViaProperty() {
		@DBTableName("Customer")
		class MyClass extends DBRow {

			@DBColumn
			public DBInteger intField1 = new DBInteger();
		}

		var property = propertyDefinitionOf(new MyClass(), "intField1");
		assertThat(property.tableName(), is("Customer"));
	}

	private PropertyWrapperDefinition<?,?> propertyDefinitionOf(RowDefinition target, String javaPropertyName) {
		return propertyDefinitionOf(target.getClass(), javaPropertyName);
	}

	// note: intentionally doesn't use a wrapper factory for tests on equals() methods
	private PropertyWrapperDefinition<?,?> propertyDefinitionOf(Class<? extends RowDefinition> clazz, String javaPropertyName) {
		var classWrapper = new RowDefinitionClassWrapper<>(clazz);
		return classWrapper.getPropertyDefinitionByName(javaPropertyName);
	}
}
