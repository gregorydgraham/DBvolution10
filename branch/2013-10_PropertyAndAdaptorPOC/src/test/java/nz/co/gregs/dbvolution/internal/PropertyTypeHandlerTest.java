package nz.co.gregs.dbvolution.internal;

import static nz.co.gregs.dbvolution.internal.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import nz.co.gregs.dbvolution.DBPebkacException;
import nz.co.gregs.dbvolution.DBTypeAdaptor;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

import org.junit.Test;

// TODO: looks like I can use this library, or base some stuff of it:
// com.sun.jersey.core.reflection.ReflectionHelper
public class PropertyTypeHandlerTest {
	private JavaPropertyFinder finder = new JavaPropertyFinder();

	// TODO: test that a sensible error is given early on where a TypeAdaptor
	//       takes the wrong arguments.

	@Test//(expected=DBPebkacException.class)
	public void errorsOnConstructionGivenTypeAdaptorWithWrongDBvType() {
		List<JavaProperty> properties = finder.getPropertiesOf(MyTable.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("fieldAdaptedToWrongDBvType")));
		new PropertyTypeHandler(property);
	}

	@Test(expected=DBPebkacException.class)
	public void errorsOnConstructionGivenInterfaceTypeAdaptor() {
		List<JavaProperty> properties = finder.getPropertiesOf(MyTable.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("interfaceAdaptorField")));
		new PropertyTypeHandler(property);
	}
	
	@Test
	public void acceptsOnConstructionGivenTypeAdaptorWithCorrectDBvType() {
		List<JavaProperty> properties = finder.getPropertiesOf(MyTable.class);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName("correctlyAdaptedField")));
		new PropertyTypeHandler(property);
	}
	
	// investigate actual java calling behaviour
	// (Theory is that, in the face of method overloading, the toObjectValue() method
	//  is based on the declared implemented TypeAdaptor generics will be the one actually
	//  invoked, even after casting to DBTypeAdaptor<Object,QueryableDatatype>).
	// (I have no idea how java actually figures this out)
	
	// Pass the type matching the 'implements' declaration and you get the correct method
	@Test
	public void invokedMethodBasedOnInterfaceGenericsGivenIntegerGenerics() {
		@SuppressWarnings("unchecked")
		DBTypeAdaptor<Object,QueryableDatatype> typeAdaptor =
				(DBTypeAdaptor<Object,QueryableDatatype>)(Object)
				new MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods();
		TypeTestUtils.describeClass(typeAdaptor.getClass());
		
		try {
			typeAdaptor.toObjectValue(new DBInteger());
		} catch (Exception e) {
			assertThat(e.getMessage(), is("toObjectValue(DBInteger): Integer"));
		}
	}
	
	// Pass the wrong type and you still get the same method as above
	// (here, we can't cast down from Number to Integer)
	@Test
	public void invokedMethodBasedOnInterfaceGenericsGivenIntegerGenericsEvenWhenGivenNumberValue() {
		@SuppressWarnings("unchecked")
		DBTypeAdaptor<Object,QueryableDatatype> typeAdaptor =
				(DBTypeAdaptor<Object,QueryableDatatype>)(Object)
				new MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods();
		TypeTestUtils.describeClass(typeAdaptor.getClass());
		
		try {
			typeAdaptor.toObjectValue(new DBNumber());
		} catch (Exception e) {
			assertThat(e, is(instanceOf(ClassCastException.class)));
			assertThat(e.getMessage(), matchesRegex(".*DBNumber cannot be cast to .*DBInteger"));
		}
	}
	
	// Pass the type matching the 'implements' declaration and you get the correct method
	@Test
	public void invokedMethodBasedOnInterfaceGenericsGivenNumberGenerics() {
		@SuppressWarnings("unchecked")
		DBTypeAdaptor<Object,QueryableDatatype> typeAdaptor =
				(DBTypeAdaptor<Object,QueryableDatatype>)(Object)
				new MyNumberDBNumberAdaptorWithIntegerDBIntegerMethods();
		TypeTestUtils.describeClass(typeAdaptor.getClass());
		
		try {
			typeAdaptor.toObjectValue(new DBNumber());
		} catch (Exception e) {
			assertThat(e.getMessage(), is("toObjectValue(DBNumber): Number"));
		}
	}

	// Pass the wrong type and you still get the same method as above
	// (here, it successfully casts up from Integer to Number)
	@Test
	public void invokedMethodBasedOnInterfaceGenericsGivenNumberGenericsEvenWhenGivenIntegerValue() {
		@SuppressWarnings("unchecked")
		DBTypeAdaptor<Object,QueryableDatatype> typeAdaptor =
				(DBTypeAdaptor<Object,QueryableDatatype>)(Object)
				new MyNumberDBNumberAdaptorWithIntegerDBIntegerMethods();
		TypeTestUtils.describeClass(typeAdaptor.getClass());
		
		try {
			typeAdaptor.toObjectValue(new DBInteger());
		} catch (Exception e) {
			assertThat(e.getMessage(), is("toObjectValue(DBNumber): Number"));
		}
	}
	
	@Test
	public void printMethods() {
		TypeTestUtils.describeClass(MyIntegerDBIntegerAdaptor.class);
		TypeTestUtils.describeClass(MyNumberDBNumberAdaptor.class);
		TypeTestUtils.describeClass(MyNumberDBNumberAdaptor2.class);
		TypeTestUtils.describeClass(MyObjectQDTAdaptor.class);
		TypeTestUtils.describeClass(MyAbstractAdaptor.class);
		TypeTestUtils.describeClass(MyAbstractAdaptor2.class);
		TypeTestUtils.describeClass(MyVarargsAdaptor.class);
		TypeTestUtils.describeClass(DBTypeAdaptor.class);
		TypeTestUtils.describeClass(MyInterfaceAdaptor.class);
		//TypeTestUtils.describeClass(MyGenericNumberInterfaceAdaptor.class);
		TypeTestUtils.describeClass(MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods.class);
		TypeTestUtils.describeClass(MyNumberDBNumberAdaptorWithIntegerDBIntegerMethods.class);
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
		public Integer toObjectValue(DBInteger dbvValue) {
			return null;
		}

		public DBInteger toDBvValue(Integer objectValue) {
			return null;
		}
	}

	public static class MyNumberDBNumberAdaptor implements DBTypeAdaptor<Number, DBNumber> {
		public Number toObjectValue(DBNumber dbvValue) {
			return null;
		}

		public DBNumber toDBvValue(Number objectValue) {
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

	public static class MyObjectQDTAdaptor implements DBTypeAdaptor<Object, QueryableDatatype> {
		public Object toObjectValue(QueryableDatatype dbvValue) {
			return null;
		}

		public QueryableDatatype toDBvValue(Object objectValue) {
			return null;
		}
	}

	public static abstract class MyAbstractAdaptor implements DBTypeAdaptor<Object, QueryableDatatype> {
		public abstract Object toObjectValue(QueryableDatatype dbvValue);

		public QueryableDatatype toDBvValue(Object objectValue) {
			return null;
		}
	}

	public static class MyAbstractAdaptor2 extends MyAbstractAdaptor {
		public Number toObjectValue(QueryableDatatype dbvValue) {
			return null;
		}
	}

	public static class MyVarargsAdaptor implements DBTypeAdaptor<Object, QueryableDatatype> {
		public Object toObjectValue(QueryableDatatype dbvValue) {
			return null;
		}

		public Object toObjectValue(QueryableDatatype... dbvValue) {
			return null;
		}

		public QueryableDatatype toDBvValue(Object objectValue) {
			return null;
		}
	}
	
	public static interface MyInterfaceAdaptor extends DBTypeAdaptor<Object, QueryableDatatype> {
	}

	public static class MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods implements DBTypeAdaptor<Integer, DBInteger> {
		@Override
		public Integer toObjectValue(DBInteger dbvValue) {
			throw new UnsupportedOperationException("toObjectValue(DBInteger): Integer");
		}

		//@Override
		public Number toObjectValue(DBNumber dbvValue) {
			throw new UnsupportedOperationException("toObjectValue(DBNumber): Number");
		}

		@Override
		public DBInteger toDBvValue(Integer objectValue) {
			throw new UnsupportedOperationException("toDBvValue(Integer): DBInteger");
		}
		
		//@Override
		public DBNumber toDBvValue(Number objectValue) {
			throw new UnsupportedOperationException("toDBvValue(Number): DBNumber");
		}
	}

	public static class MyNumberDBNumberAdaptorWithIntegerDBIntegerMethods implements DBTypeAdaptor<Number, DBNumber> {
		//@Override
		public Integer toObjectValue(DBInteger dbvValue) {
			throw new UnsupportedOperationException("toObjectValue(DBInteger): Integer");
		}

		@Override
		public Number toObjectValue(DBNumber dbvValue) {
			throw new UnsupportedOperationException("toObjectValue(DBNumber): Number");
		}

		//@Override
		public DBInteger toDBvValue(Integer objectValue) {
			throw new UnsupportedOperationException("toDBvValue(Integer): DBInteger");
		}
		
		@Override
		public DBNumber toDBvValue(Number objectValue) {
			throw new UnsupportedOperationException("toDBvValue(Number): DBNumber");
		}
	}
	
//	public static class DualImplementation implements DBTypeAdaptor<Number, DBNumber>, DBTypeAdaptor<Integer, DBInteger> {
//		//@Override
//		public Integer toObjectValue(DBInteger dbvValue) {
//			throw new UnsupportedOperationException("toObjectValue(DBInteger): Integer");
//		}
//
//		//@Override
//		public Number toObjectValue(DBNumber dbvValue) {
//			throw new UnsupportedOperationException("toObjectValue(DBNumber): Number");
//		}
//
//		//@Override
//		public DBInteger toDBvValue(Integer objectValue) {
//			throw new UnsupportedOperationException("toDBvValue(Integer): DBInteger");
//		}
//		
//		//@Override
//		public DBNumber toDBvValue(Number objectValue) {
//			throw new UnsupportedOperationException("toDBvValue(Number): DBNumber");
//		}
//	}
}
