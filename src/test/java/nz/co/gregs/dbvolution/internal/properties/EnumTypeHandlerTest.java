package nz.co.gregs.dbvolution.internal.properties;

import static nz.co.gregs.dbvolution.internal.properties.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import junit.framework.AssertionFailedError;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBEnum;
import nz.co.gregs.dbvolution.datatypes.DBEnumValue;
import nz.co.gregs.dbvolution.exceptions.InvalidDeclaredTypeException;
import nz.co.gregs.dbvolution.internal.properties.JavaPropertyFinder.PropertyType;
import nz.co.gregs.dbvolution.internal.properties.JavaPropertyFinder.Visibility;

import org.junit.Test;

@SuppressWarnings({"serial", "unused"})
public class EnumTypeHandlerTest {

	private final JavaPropertyFinder privateFieldPublicBeanFinder = new JavaPropertyFinder(
			Visibility.PRIVATE, Visibility.PUBLIC, null, (PropertyType[]) null);

	@Test
	public void acceptsInvalidDeclarationGivenNonColumn() {
		class TestClass extends DBRow {

			public DBEnum<?, ?> field;
		}

		try {
			typeHandlerOf(TestClass.class, "field");
		} catch (InvalidDeclaredTypeException e) {
			Error error = new AssertionFailedError("Encountered " + e.getClass().getSimpleName() + ", expected no exception");
			error.initCause(e);
			throw error;
		}
	}

	@Test
	public void acceptsValidDeclarationGivenColumn() {
		class TestClass extends DBRow {

			@DBColumn
			public DBEnum<MyIntegerEnum, Integer> field;
		}

		try {
			typeHandlerOf(TestClass.class, "field");
		} catch (InvalidDeclaredTypeException e) {
			Error error = new AssertionFailedError("Encountered " + e.getClass().getSimpleName() + ", expected no exception");
			error.initCause(e);
			throw error;
		}
	}

	@Test(expected = InvalidDeclaredTypeException.class)
	public void rejectsInvalidDeclarationGivenColumnAndNoGenerics() {
		@SuppressWarnings("rawtypes")
		class TestClass extends DBRow {

			@DBColumn
			public DBEnum field;
		}

		typeHandlerOf(TestClass.class, "field");
	}

	@Test(expected = InvalidDeclaredTypeException.class)
	public void rejectsInvalidDeclarationGivenColumnAndWildcardGenerics() {
		class TestClass extends DBRow {

			@DBColumn
			public DBEnum<?, ?> field;
		}

		typeHandlerOf(TestClass.class, "field");
	}

	@Test(expected = InvalidDeclaredTypeException.class)
	public void rejectsInvalidDeclarationGivenColumnAndWildcardGenerics2() {
		class TestClass extends DBRow {

			@DBColumn
			public DBEnum<? extends MyIntegerEnum, Integer> field;
		}

		typeHandlerOf(TestClass.class, "field");
	}

	@Test
	public void infersEnumTypeGivenIntegerEnum() {
		class TestClass extends DBRow {

			@DBColumn
			public DBEnum<MyIntegerEnum, Integer> field;
		}

		EnumTypeHandler enumTypeHandler = typeHandlerOf(TestClass.class, "field");
		assertThat(enumTypeHandler.getEnumType(), is((Object) MyIntegerEnum.class));
	}

	@Test
	public void infersEnumLiteralValueTypeGivenIntegerEnum() {
		class TestClass extends DBRow {

			@DBColumn
			public DBEnum<MyIntegerEnum, Integer> field;
		}

		EnumTypeHandler enumTypeHandler = typeHandlerOf(TestClass.class, "field");
		assertThat(enumTypeHandler.getEnumLiteralValueType(), is((Object) Integer.class));
	}

	@Test
	public void infersEnumLiteralValueTypeGivenStringEnum() {
		class TestClass extends DBRow {

			@DBColumn
			public DBEnum<MyStringEnum, String> field;
		}

		EnumTypeHandler enumTypeHandler = typeHandlerOf(TestClass.class, "field");
		assertThat(enumTypeHandler.getEnumLiteralValueType(), is((Object) String.class));
	}

	private enum MyIntegerEnum implements DBEnumValue<Integer> {

		ZERO, ONE, TWO;

		@Override
		public Integer getCode() {
			return ordinal();
		}
	}

	private enum MyStringEnum implements DBEnumValue<String> {

		ZERO, ONE, TWO;

		@Override
		public String getCode() {
			return name().toLowerCase();
		}
	}

	private EnumTypeHandler typeHandlerOf(Class<?> clazz, String javaPropertyName) {
		ColumnHandler columnHandler = new ColumnHandler(propertyOf(clazz, javaPropertyName));
		return new EnumTypeHandler(propertyOf(clazz, javaPropertyName), columnHandler);
	}

	private JavaProperty propertyOf(Class<?> clazz, String javaPropertyName) {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(clazz);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName(javaPropertyName)));
		if (property == null) {
			throw new IllegalArgumentException("No property found with java name '" + javaPropertyName + "'");
		}
		return property;
	}
}
