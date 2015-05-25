package nz.co.gregs.dbvolution.internal.properties;

import static nz.co.gregs.dbvolution.internal.properties.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.internal.properties.JavaPropertyFinder.PropertyType;
import nz.co.gregs.dbvolution.internal.properties.JavaPropertyFinder.Visibility;

import org.junit.Test;

public class ColumnHandlerTest {

	private final JavaPropertyFinder privateFieldPublicBeanFinder = new JavaPropertyFinder(
			Visibility.PRIVATE, Visibility.PUBLIC, null, (PropertyType[]) null);

	@Test
	public void isColumnWhenEmptyColumnAnnotationIsPresentOnSimpleField() {
		class TestClass {

			@DBColumn
			public int field;
		}

		ColumnHandler handler = columnHandlerOf(TestClass.class, "field");
		assertThat(handler.isColumn(), is(true));
	}

	@Test
	public void isColumnWhenEmptyColumnAnnotationIsPresentOnSimpleBeanProperty() {
		class TestClass {

			@DBColumn
			public int getProperty() {
				return 0;
			}
		}

		ColumnHandler handler = columnHandlerOf(TestClass.class, "property");
		assertThat(handler.isColumn(), is(true));
	}

	@Test
	public void isColumnWhenNonEmptyColumnAnnotationIsPresentOnSimpleField() {
		class TestClass {

			@DBColumn("foo")
			public int field;
		}

		ColumnHandler handler = columnHandlerOf(TestClass.class, "field");
		assertThat(handler.isColumn(), is(true));
	}

	@Test
	public void notColumnWhenMissingColumnAnnotation() {
		class TestClass {

			public DBInteger field;
		}

		ColumnHandler handler = columnHandlerOf(TestClass.class, "field");
		assertThat(handler.isColumn(), is(false));
	}

	@Test
	public void isPrimaryKeyGivenColumnAndPrimaryKeyAnnotations() {
		class TestClass {

			@DBColumn
			@DBPrimaryKey
			public DBInteger field;
		}

		ColumnHandler handler = columnHandlerOf(TestClass.class, "field");
		assertThat(handler.isPrimaryKey(), is(true));
	}

	@Test
	public void notPrimaryKeyGivenPrimaryKeyAnnotationButMissingColumnAnnotation() {
		class TestClass {

			@DBPrimaryKey
			public DBInteger field;
		}

		ColumnHandler handler = columnHandlerOf(TestClass.class, "field");
		assertThat(handler.isPrimaryKey(), is(false));
	}

	private ColumnHandler columnHandlerOf(Class<?> clazz, String javaPropertyName) {
		return new ColumnHandler(propertyOf(clazz, javaPropertyName));
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
