package nz.co.gregs.dbvolution.internal;

import static nz.co.gregs.dbvolution.internal.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;

import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings({"serial","unused"})
public class PropertyTypeHandlerTest {
	private JavaPropertyFinder finder = new JavaPropertyFinder();

	@Ignore // not working yet
	@Test(expected=DBPebkacException.class)
	public void errorsOnConstructionGivenTypeAdaptorWithWrongDBvType() {
		List<JavaProperty> properties = finder.getPropertiesOf(MyTable.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("fieldAdaptedToWrongDBvType")));
		new PropertyTypeHandler(property, false);
	}

	@Test(expected=DBPebkacException.class)
	public void errorsOnConstructionGivenInterfaceTypeAdaptor() {
		List<JavaProperty> properties = finder.getPropertiesOf(MyTable.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("interfaceAdaptorField")));
		new PropertyTypeHandler(property, false);
	}
	
	@Test
	public void acceptsOnConstructionGivenTypeAdaptorWithCorrectDBvType() {
		List<JavaProperty> properties = finder.getPropertiesOf(MyTable.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("correctlyAdaptedField")));
		new PropertyTypeHandler(property, false);
	}
	
	@Test
	public void infersDBStringGivenIntegerStringAdaptor() {
		class MyClass extends DBRow {
			@DBAdaptType(adaptor=LongStringAdaptor.class)
			@DBColumn
			public DBInteger intField = new DBInteger();
		}
		
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");
		assertThat(propertyHandler.getType(), is((Object) DBString.class));
	}
	
	@Test
	public void getsQDTValueGivenValidFieldAndNoTypeAdaptor() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField = new DBInteger();
		}
		
		MyClass myObj = new MyClass();
		myObj.intField.setValue(23);
		
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");
		DBInteger qdt = (DBInteger)propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		assertThat(qdt.intValue(), is(23));
	}

	@Test
	public void getsUnchangedQDTInstanceGivenValidFieldAndNoTypeAdaptor() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField = new DBInteger();
		}
		
		MyClass myObj = new MyClass();
		
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");
		DBInteger qdt = (DBInteger)propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		assertThat(qdt == myObj.intField, is(true));
	}
	
	@Test
	public void getsNullQDTValueGivenValidNullFieldAndNoTypeAdaptor() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField = null;
		}
		
		MyClass myObj = new MyClass();
		
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");
		DBInteger qdt = (DBInteger)propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		assertThat(qdt, is(nullValue()));
	}
	
	@Test
	public void getsCorrectInternalValueTypeGivenIntegerStringAdaptorOnDBIntegerField() {
		class MyClass extends DBRow {
			@DBAdaptType(adaptor=LongStringAdaptor.class)
			@DBColumn
			public DBInteger intField = new DBInteger();
		}
		
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");
		QueryableDatatype qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(qdt, is(instanceOf(DBString.class)));
	}

	@Test
	public void getsCorrectInternalValueGivenLongStringAdaptorOnDBIntegerField() {
		class MyClass extends DBRow {
			@DBAdaptType(adaptor=LongStringAdaptor.class)
			@DBColumn
			public DBInteger intField = new DBInteger();
		}
		
		MyClass myObj = new MyClass();
		myObj.intField.setValue(23);
		
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");
		DBString qdt = (DBString)propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		
		assertThat(qdt.stringValue(), is("23"));
	}
	
	@Test
	public void setsFieldValueGivenValidFieldAndNoTypeAdaptor() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField = new DBInteger();
		}
		
		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");
		
		DBInteger qdt = new DBInteger();
		qdt.setValue(23);
		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, qdt);
		
		assertThat(myObj.intField.intValue(), is(23));
	}

	@Test
	public void setsUnchangedFieldReferenceGivenValidObjectAndNoTypeAdaptor() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField = new DBInteger();
		}
		
		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");

		DBInteger qdt = new DBInteger();
		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, qdt);
		
		assertThat(myObj.intField == qdt, is(true));
	}
	
	@Test
	public void setsFieldValueToNullGivenValidObjectAndNoTypeAdaptor() {
		class MyClass extends DBRow {
			@DBColumn
			public DBInteger intField = new DBInteger();
		}
		
		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "intField");

		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, null);
		assertThat(myObj.intField, is(nullValue()));
	}

	private PropertyTypeHandler propertyHandlerOf(Class<?> clazz, String javaPropertyName) {
		return new PropertyTypeHandler(propertyOf(clazz, javaPropertyName), false);
	}
	
	private JavaProperty propertyOf(Class<?> clazz, String javaPropertyName) {
		List<JavaProperty> properties = finder.getPropertiesOf(clazz);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName(javaPropertyName)));
		if (property == null) {
			throw new IllegalArgumentException("No public property found with java name '"+javaPropertyName+"'");
		}
		return property;
	}
	
	public static class LongStringAdaptor implements DBTypeAdaptor<Long,String> {
		@Override
		public Long fromDatabaseValue(String dbvValue) {
			if (dbvValue != null) {
				return Long.parseLong(dbvValue);
			}
			return null;
		}

		@Override
		public String toDatabaseValue(Long objectValue) {
			if (objectValue != null) {
				return objectValue.toString();
			}
			return null;
		}
	}
	
	@DBTableName("Simple_Table")
	public static class MyTable {
		@DBColumn
		@DBAdaptType(adaptor=MyIntegerDBIntegerAdaptor.class, type=DBString.class)
		public Integer fieldAdaptedToWrongDBvType;

		@DBColumn
		@DBAdaptType(adaptor=MyIntegerDBIntegerAdaptor.class, type=DBInteger.class)
		public Integer correctlyAdaptedField;
		
		@DBColumn
		@DBAdaptType(adaptor=MyInterfaceAdaptor.class, type=DBInteger.class)
		public DBInteger interfaceAdaptorField;
	}
	
	public static class MyIntegerDBIntegerAdaptor implements DBTypeAdaptor<Integer, DBInteger> {
		public Integer fromDatabaseValue(DBInteger dbvValue) {
			return null;
		}

		public DBInteger toDatabaseValue(Integer objectValue) {
			return null;
		}
	}

	public static class MyNumberDBNumberAdaptor implements DBTypeAdaptor<Number, DBNumber> {
		public Number fromDatabaseValue(DBNumber dbvValue) {
			return null;
		}

		public DBNumber toDatabaseValue(Number objectValue) {
			return null;
		}
	}
	
	public static class MyNumberDBNumberAdaptor2 extends MyNumberDBNumberAdaptor {
		public Integer toObjectValue(DBInteger dbvValue) {
			return null;
		}

		public String toObjectValue(String dbvValue) {
			return null;
		}
		
		public DBInteger toDBvValue(Integer objectValue) {
			return null;
		}
	}
	
	public static interface MyInterfaceAdaptor extends DBTypeAdaptor<Object, QueryableDatatype> {
	}
}
