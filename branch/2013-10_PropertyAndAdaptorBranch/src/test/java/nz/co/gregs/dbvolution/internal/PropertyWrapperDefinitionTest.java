package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBInteger;

import org.junit.Test;

public class PropertyWrapperDefinitionTest {
	@Test
	public void dotEqualsTrueWhenDifferentObjectAndSameClass() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField1 = new DBInteger();

			@DBColumn
			public DBInteger intField2 = new DBInteger();
		}
		
		PropertyWrapperDefinition intField1_obj1 = wrapperDefinitionOf(new MyClass(), "intField1");
		PropertyWrapperDefinition intField1_obj2 = wrapperDefinitionOf(new MyClass(), "intField1");
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
		
		PropertyWrapperDefinition intField1_obj1 = wrapperDefinitionOf(new MyClass1(), "intField1");
		PropertyWrapperDefinition intField1_obj2 = wrapperDefinitionOf(new MyClass2(), "intField1");
		assertThat(intField1_obj1.equals(intField1_obj2), is(false));
	}
	
	private PropertyWrapperDefinition wrapperDefinitionOf(Object target, String javaPropertyName) {
		return wrapperDefinitionOf(target.getClass(), javaPropertyName);
	}
	
	// note: intentionally doesn't use a wrapper factory for tests on equals() methods
	private PropertyWrapperDefinition wrapperDefinitionOf(Class<?> clazz, String javaPropertyName) {
		DBRowClassWrapper classWrapper = new DBRowClassWrapper(clazz);
		return classWrapper.getPropertyByName(javaPropertyName);
	}
}
