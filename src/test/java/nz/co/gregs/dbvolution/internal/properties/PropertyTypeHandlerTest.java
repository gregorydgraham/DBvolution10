package nz.co.gregs.dbvolution.internal.properties;

import static nz.co.gregs.dbvolution.internal.properties.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBStringTrimmed;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.InvalidDeclaredTypeException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Focuses on low-level functionality of type adaptors. End-to-end confirmation
 * that it works when querying an actual database is in {@link TypeAdaptorTest}.
 */
@SuppressWarnings({"serial", "unused"})
public class PropertyTypeHandlerTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void errorsOnConstructionGivenValidTypeAdaptorWithWrongExplicitDBvType() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = StringLongAdaptor.class, type = DBString.class)
			public DBString field = new DBString();
		}

		thrown.expect(InvalidDeclaredTypeException.class);
		thrown.expectMessage("internal Long type is not compatible");
		PropertyTypeHandler propertyTypeHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
	}

	@Test
	public void errorsOnConstructionGivenTypeAdaptorWithWrongExternalType() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = LongStringAdaptor.class)
			public DBString field = new DBString();
		}

		thrown.expect(InvalidDeclaredTypeException.class);
		thrown.expectMessage("external Long type is not compatible");
		PropertyTypeHandler propertyTypeHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
	}

	@Test
	public void errorsOnConstructionGivenInvalidAdaptorWithNonSimpleFirstType() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = DBStringIntegerAdaptor.class, type = DBInteger.class)
			public DBString field = new DBString();
		}

		thrown.expect(InvalidDeclaredTypeException.class);
		thrown.expectMessage("external type must not");
		PropertyTypeHandler propertyTypeHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
	}

	@Test
	public void errorsOnConstructionGivenInvalidAdaptorWithNonSimpleSecondType() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = IntegerDBIntegerAdaptor.class, type = DBInteger.class)
			public DBString field = new DBString();
		}

		thrown.expect(InvalidDeclaredTypeException.class);
		thrown.expectMessage("internal type must not");
		PropertyTypeHandler propertyTypeHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
	}

	@Test(expected = InvalidDeclaredTypeException.class)
	public void errorsOnConstructionGivenInterfaceTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = AdaptorInterface.class, type = DBInteger.class)
			public DBInteger field;
		}

		PropertyTypeHandler propertyTypeHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
	}

	@Test
	public void acceptsOnConstructionGivenValidTypeAdaptorWithImplicitDBvType() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = IntegerLongAdaptor.class)
			public Integer field;
		}

		PropertyTypeHandler propertyHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
		assertThat(propertyHandler, is(not(nullValue())));
	}

	@Test
	public void acceptsOnConstructionGivenValidTypeAdaptorWithCorrectExplicitDBvType() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = IntegerLongAdaptor.class, type = DBInteger.class)
			public Integer field;
		}

		PropertyTypeHandler propertyHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
		assertThat(propertyHandler, is(not(nullValue())));
	}

	@Test
	public void acceptsOnConstructionGivenValidTypeAdaptorWithExternalSimpleTypeUpcast() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = IntegerStringAdaptor.class)
			public Long field;
		}

		PropertyTypeHandler propertyHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
		assertThat(propertyHandler, is(not(nullValue())));
	}

	@Test
	public void acceptsOnConstructionGivenValidTypeAdaptorWithExternalSimpleTypeDowncast() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = LongStringAdaptor.class)
			public Integer field;
		}

		PropertyTypeHandler propertyHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
		assertThat(propertyHandler, is(not(nullValue())));
	}

	@Test
	public void acceptsOnConstructionGivenValidTypeAdaptorWithInternalSimpleTypeUpcast() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = StringIntegerAdaptor.class)
			public String field;
		}

		PropertyTypeHandler propertyHandler = new PropertyTypeHandler(propertyOf(MyClass.class, "field"), false);
		assertThat(propertyHandler, is(not(nullValue())));
	}

	@Test
	public void infersDBIntegerGivenStringLongAdaptorOnDBStringField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = StringLongAdaptor.class)
			@DBColumn
			public DBString field = new DBString("23");
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBInteger.class));
		assertThat(qdt, is(instanceOf(DBInteger.class)));
		assertThat(qdt.getValue(), is(instanceOf(Long.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBInteger(42));
	}

	@Test
	public void infersDBIntegerGivenStringTrimmedLongAdaptorOnDBStringField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = StringTrimmedLongAdaptor.class)
			@DBColumn
			public DBStringTrimmed field = new DBStringTrimmed("23");
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBInteger.class));
		assertThat(qdt, is(instanceOf(DBInteger.class)));
		assertThat(qdt.getValue(), is(instanceOf(Long.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBInteger(42));
	}

	@Test
	public void infersDBIntegerGivenStringLongAdaptorOnStringField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = StringLongAdaptor.class)
			@DBColumn
			public String field = "23";
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBInteger.class));
		assertThat(qdt, is(instanceOf(DBInteger.class)));
		assertThat(qdt.getValue(), is(instanceOf(Long.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBInteger(42));
	}

	@Test
	public void infersDBIntegerGivenStringIntegerAdaptorOnDBStringField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = StringIntegerAdaptor.class)
			@DBColumn
			public DBString field = new DBString("23");
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBInteger.class));
		assertThat(qdt, is(instanceOf(DBInteger.class)));
		assertThat(qdt.getValue(), is(instanceOf(Long.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBInteger(42));
	}

	@Test
	public void infersDBIntegerGivenStringIntegerAdaptorOnStringField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = StringIntegerAdaptor.class)
			@DBColumn
			public String field = "23";
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBInteger.class));
		assertThat(qdt, is(instanceOf(DBInteger.class)));
		assertThat(qdt.getValue(), is(instanceOf(Long.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBInteger(42));
	}

	@Test
	public void infersDBStringGivenLongStringAdaptorOnDBIntegerField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = LongStringAdaptor.class)
			@DBColumn
			public DBInteger field = new DBInteger(23);
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBString.class));
		assertThat(qdt, is(instanceOf(DBString.class)));
		assertThat(qdt.getValue(), is(instanceOf(String.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBString("42"));
	}

	@Test
	public void infersDBStringGivenLongStringAdaptorOnLongField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = LongStringAdaptor.class)
			@DBColumn
			public Long field = 23L;
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBString.class));
		assertThat(qdt, is(instanceOf(DBString.class)));
		assertThat(qdt.getValue(), is(instanceOf(String.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBString("42"));
	}

	@Test
	public void infersDBStringGivenLongStringAdaptorOnIntegerField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = LongStringAdaptor.class)
			@DBColumn
			public Integer field = 23;
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBString.class));
		assertThat(qdt, is(instanceOf(DBString.class)));
		assertThat(qdt.getValue(), is(instanceOf(String.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBString("42"));
	}

	@Test
	public void infersDBStringGivenIntegerStringAdaptorOnLongField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = IntegerStringAdaptor.class)
			@DBColumn
			public Long field = 23L;
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBString.class));
		assertThat(qdt, is(instanceOf(DBString.class)));
		assertThat(qdt.getValue(), is(instanceOf(String.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBString("42"));
	}

	@Test
	public void infersLongGivenDateLongAdaptor() {
		class MyClass extends DBRow {

			@DBAdaptType(value = DateLongAdaptor.class)
			@DBColumn
			public DBDate field = new DBDate(new Date());
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBInteger.class));
		assertThat(qdt, is(instanceOf(DBInteger.class)));
		assertThat(qdt.getValue(), is(instanceOf(Long.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBInteger(42));
	}

	@Test
	public void infersDBDateGivenIntegerStringAdaptor() {
		class MyClass extends DBRow {

			@DBAdaptType(value = LongDateAdaptor.class)
			@DBColumn
			public Long field = 2013L;
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBDate.class));
		assertThat(qdt, is(instanceOf(DBDate.class)));
		assertThat(qdt.getValue(), is(instanceOf(Date.class)));
		propertyHandler.setJavaPropertyAsQueryableDatatype(new MyClass(), new DBDate(new Date()));
	}

	@Test
	public void acceptsDBIntegerGivenIntegerAdaptorAndExplicitType() {
		class MyClass extends DBRow {

			@DBAdaptType(value = StringIntegerAdaptor.class, type = DBInteger.class)
			@DBColumn
			public String field = "23";
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(propertyHandler.getQueryableDatatypeClass(), is((Object) DBInteger.class));
		assertThat(qdt, is(instanceOf(DBInteger.class)));
		assertThat(qdt.getValue(), is(instanceOf(Long.class)));
	}

	@Test
	public void getsQDTValueGivenValidFieldAndNoTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();
		myObj.field.setValue(23);

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		DBInteger qdt = (DBInteger) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		assertThat(qdt.getValue().intValue(), is(23));
	}

	@Test
	public void getsUnchangedQDTInstanceGivenValidFieldAndNoTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		DBInteger qdt = (DBInteger) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		assertThat(qdt == myObj.field, is(true));
	}

	@Test
	public void getsNullQDTValueGivenValidNullQDTFieldAndNoTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			public DBInteger field = null;
		}

		MyClass myObj = new MyClass();

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		DBInteger qdt = (DBInteger) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		assertThat(qdt, is(nullValue()));
	}

	@Test
	public void getsNullAdaptedQDTValueGivenNullQDTFieldAndTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = LongStringAdaptor.class)
			public DBInteger field = null;
		}

		MyClass myObj = new MyClass();

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		assertThat(qdt, is(nullValue()));
	}

	@Test
	public void getsIsNullAdaptedQDTValueGivenNullSimpleFieldAndTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = LongStringAdaptor.class)
			public Long field = null;
		}

		MyClass myObj = new MyClass();

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		assertThat(qdt, is(not(nullValue())));
		assertThat(qdt.isNull(), is(true));
	}

	@Test
	public void getsCorrectInternalValueTypeGivenIntegerStringAdaptorOnDBIntegerField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = LongStringAdaptor.class)
			@DBColumn
			public DBInteger field = new DBInteger();
		}

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		QueryableDatatype<?> qdt = propertyHandler.getJavaPropertyAsQueryableDatatype(new MyClass());
		assertThat(qdt, is(instanceOf(DBString.class)));
	}

	@Test
	public void getsCorrectInternalValueGivenLongStringAdaptorOnDBIntegerField() {
		class MyClass extends DBRow {

			@DBAdaptType(value = LongStringAdaptor.class)
			@DBColumn
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();
		myObj.field.setValue(23);

		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		DBString qdt = (DBString) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);

		assertThat(qdt.stringValue(), is("23"));
	}

	@Test
	public void getsSameInstanceOnConsecutiveReadsGivenAdaptorWhenUsingSameHandler() {
		class MyClass extends DBRow {

			@DBAdaptType(value = LongStringAdaptor.class)
			@DBColumn
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		DBString qdt1 = (DBString) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		DBString qdt2 = (DBString) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);
		DBString qdt3 = (DBString) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);

		assertThat(qdt2 == qdt1, is(true));
		assertThat(qdt3 == qdt1, is(true));
	}

	@Test
	public void getsDifferentInstanceOnConsecutiveReadsAndWritesGivenAdaptorWhenUsingSameHandler() {
		class MyClass extends DBRow {

			@DBAdaptType(value = LongStringAdaptor.class)
			@DBColumn
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		DBString qdt1 = (DBString) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);

		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, new DBString());
		DBString qdt2 = (DBString) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);

		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, new DBString());
		DBString qdt3 = (DBString) propertyHandler.getJavaPropertyAsQueryableDatatype(myObj);

		assertThat(qdt2 == qdt1, is(false));
		assertThat(qdt3 == qdt1, is(false));
		assertThat(qdt3 == qdt2, is(false));
	}

	@Test
	public void setsFieldValueGivenValidFieldAndNoTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");

		DBInteger qdt = new DBInteger();
		qdt.setValue(23);
		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, qdt);

		assertThat(myObj.field.getValue().intValue(), is(23));
	}

	@Test
	public void setsFieldValueGivenValidFieldAndTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(StringLongAdaptor.class)
			public DBString field = new DBString();
		}

		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");

		DBInteger qdt = new DBInteger();
		qdt.setValue(23);
		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, qdt);

		assertThat(myObj.field.stringValue(), is("23"));
	}

	@Test
	public void setsUnchangedFieldReferenceGivenValidObjectAndNoTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");

		DBInteger qdt = new DBInteger();
		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, qdt);

		assertThat(myObj.field == qdt, is(true));
	}

	@Test
	public void setsQDTFieldNullGivenNullQDTAndNoTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");

		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, null);
		assertThat(myObj.field, is(nullValue()));
	}

	@Test
	public void setsQDTFieldNullGivenNullQDTAndTypeAdaptor() {
		class MyClass extends DBRow {

			@DBColumn
			@DBAdaptType(value = IntegerStringAdaptor.class)
			public DBInteger field = new DBInteger();
		}

		MyClass myObj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");

		propertyHandler.setJavaPropertyAsQueryableDatatype(myObj, null);
		assertThat(myObj.field, is(nullValue()));
	}

	@Test
	public void setsAsDBIntegerWithLongGivenLongAdaptor() {
		class MyClass extends DBRow {

			@DBAdaptType(value = StringLongAdaptor.class)
			@DBColumn
			public DBString field = new DBString();
		}

		MyClass obj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		propertyHandler.setJavaPropertyAsQueryableDatatype(obj, new DBInteger(42L));
		assertThat(obj.field.getValue(), is((Object) "42"));
	}

	@Test
	public void setsAsDBIntegerWithIntegerGivenLongAdaptor() {
		class MyClass extends DBRow {

			@DBAdaptType(value = StringLongAdaptor.class)
			@DBColumn
			public DBString field = new DBString();
		}

		MyClass obj = new MyClass();
		PropertyTypeHandler propertyHandler = propertyHandlerOf(MyClass.class, "field");
		propertyHandler.setJavaPropertyAsQueryableDatatype(obj, new DBInteger(42));
		assertThat(obj.field.getValue(), is((Object) "42"));
	}

	private PropertyTypeHandler propertyHandlerOf(Class<?> clazz, String javaPropertyName) {
		return new PropertyTypeHandler(propertyOf(clazz, javaPropertyName), false);
	}

	private JavaProperty propertyOf(Class<?> clazz, String javaPropertyName) {
		List<JavaProperty> properties = new JavaPropertyFinder().getPropertiesOf(clazz);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName(javaPropertyName)));
		if (property == null) {
			throw new IllegalArgumentException("No public property found with java name '" + javaPropertyName + "'");
		}
		return property;
	}

	public static class IntegerStringAdaptor implements DBTypeAdaptor<Integer, String> {

		@Override
		public Integer fromDatabaseValue(String dbvValue) {
			if (dbvValue != null) {
				return Integer.parseInt(dbvValue);
			}
			return null;
		}

		@Override
		public String toDatabaseValue(Integer objectValue) {
			if (objectValue != null) {
				return objectValue.toString();
			}
			return null;
		}
	}

	public static class StringIntegerAdaptor implements DBTypeAdaptor<String, Integer> {

		@Override
		public String fromDatabaseValue(Integer dbvValue) {
			if (dbvValue != null) {
				return dbvValue.toString();
			}
			return null;
		}

		@Override
		public Integer toDatabaseValue(String objectValue) {
			if (objectValue != null) {
				return Integer.parseInt(objectValue);
			}
			return null;
		}
	}

	public static class LongStringAdaptor implements DBTypeAdaptor<Long, String> {

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

	public static class StringLongAdaptor implements DBTypeAdaptor<String, Long> {

		@Override
		public String fromDatabaseValue(Long dbvValue) {
			if (dbvValue != null) {
				return dbvValue.toString();
			}
			return null;
		}

		@Override
		public Long toDatabaseValue(String objectValue) {
			if (objectValue != null) {
				return Long.parseLong(objectValue);
			}
			return null;
		}
	}

	public static class StringTrimmedLongAdaptor implements DBTypeAdaptor<String, Long> {

		@Override
		public String fromDatabaseValue(Long dbvValue) {
			if (dbvValue != null) {
				return dbvValue.toString();
			}
			return null;
		}

		@Override
		public Long toDatabaseValue(String objectValue) {
			if (objectValue != null) {
				return Long.parseLong(objectValue);
			}
			return null;
		}
	}

	public static class DateLongAdaptor implements DBTypeAdaptor<Date, Long> {

		@Override
		public Date fromDatabaseValue(Long dbvValue) {
			if (dbvValue != null) {
				Calendar c = Calendar.getInstance();
				c.clear();
				c.set(Calendar.YEAR, dbvValue.intValue());
				return c.getTime();
			}
			return null;
		}

		@Override
		public Long toDatabaseValue(Date objectValue) {
			Calendar c = Calendar.getInstance();
			c.setTime(objectValue);
			return (long) c.get(Calendar.YEAR);
		}
	}

	public static class LongDateAdaptor implements DBTypeAdaptor<Long, Date> {

		@Override
		public Long fromDatabaseValue(Date dbvValue) {
			Calendar c = Calendar.getInstance();
			c.setTime(dbvValue);
			return (long) c.get(Calendar.YEAR);
		}

		@Override
		public Date toDatabaseValue(Long objectValue) {
			if (objectValue != null) {
				Calendar c = Calendar.getInstance();
				c.clear();
				c.set(Calendar.YEAR, objectValue.intValue());
				return c.getTime();
			}
			return null;
		}
	}

	public static class IntegerLongAdaptor implements DBTypeAdaptor<Integer, Long> {

		@Override
		public Integer fromDatabaseValue(Long dbvValue) {
			return null;
		}

		@Override
		public Long toDatabaseValue(Integer objectValue) {
			return null;
		}
	}

	public static class IntegerDBIntegerAdaptor implements DBTypeAdaptor<Integer, DBInteger> {

		@Override
		public Integer fromDatabaseValue(DBInteger dbvValue) {
			return null;
		}

		@Override
		public DBInteger toDatabaseValue(Integer objectValue) {
			return null;
		}
	}

	public static class DBStringIntegerAdaptor implements DBTypeAdaptor<DBString, Integer> {

		@Override
		public DBString fromDatabaseValue(Integer dbvValue) {
			return null;
		}

		@Override
		public Integer toDatabaseValue(DBString objectValue) {
			return null;
		}
	}

	public static interface AdaptorInterface extends DBTypeAdaptor<Object, QueryableDatatype<?>> {
		// empty
	}
}
