package nz.co.gregs.dbvolution.internal;

import static nz.co.gregs.dbvolution.internal.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
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
		printTypeAdaptorMethods(typeAdaptor.getClass());
		
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
		printTypeAdaptorMethods(typeAdaptor.getClass());
		
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
		printTypeAdaptorMethods(typeAdaptor.getClass());
		
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
		printTypeAdaptorMethods(typeAdaptor.getClass());
		
		try {
			typeAdaptor.toObjectValue(new DBInteger());
		} catch (Exception e) {
			assertThat(e.getMessage(), is("toObjectValue(DBNumber): Number"));
		}
	}
	
	@Test
	public void printMethods() {
		printTypeAdaptorMethods(MyIntegerDBIntegerAdaptor.class);
		printTypeAdaptorMethods(MyNumberDBNumberAdaptor.class);
		printTypeAdaptorMethods(MyNumberDBNumberAdaptor2.class);
		printTypeAdaptorMethods(MyObjectQDTAdaptor.class);
		printTypeAdaptorMethods(MyAbstractAdaptor.class);
		printTypeAdaptorMethods(MyAbstractAdaptor2.class);
		printTypeAdaptorMethods(MyVarargsAdaptor.class);
		printTypeAdaptorMethods(DBTypeAdaptor.class);
		printTypeAdaptorMethods(MyInterfaceAdaptor.class);
		printTypeAdaptorMethods(MyGenericNumberInterfaceAdaptor.class);
		printTypeAdaptorMethods(MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods.class);
		printTypeAdaptorMethods(MyNumberDBNumberAdaptorWithIntegerDBIntegerMethods.class);
	}

	public static void printTypeAdaptorMethods(Class<?> typeAdaptorClass) {
		System.out.println("Type adptor methods of "+typeAdaptorClass.getSimpleName());
		System.out.println("  (class: synthetic="+typeAdaptorClass.isSynthetic()+"," +
				" interface="+typeAdaptorClass.isInterface()+","+
				" abstract="+(Modifier.isAbstract(typeAdaptorClass.getModifiers()))+
				")");
		Type[] genericInterfaces = typeAdaptorClass.getGenericInterfaces();
		if (genericInterfaces != null && genericInterfaces.length > 0) {
			System.out.print("  (class: generics=");
			boolean first = true;
			for (Type type: genericInterfaces) {
				if (!first) System.out.print(",");
				if (type instanceof ParameterizedType) {
					Class<?> clazz = (Class<?>)(((ParameterizedType) type).getRawType());
					System.out.print(clazz.getSimpleName());
					Type[] args = ((ParameterizedType) type).getActualTypeArguments();
					if (args != null && args.length > 0) {
						System.out.print("<");
						boolean first2 = true;
						for (Type arg: args) {
							if (!first2) System.out.print(",");
							if (arg instanceof TypeVariable) {
								System.out.print(((TypeVariable) arg).getName());
								System.out.print(":");
								System.out.print(((Class<?>)((TypeVariable) arg).getBounds()[0]).getSimpleName());
								//System.out.print(((TypeVariable) arg).getGenericDeclaration());
							}
							else if (arg instanceof Class) {
								System.out.print(((Class) arg).getSimpleName());
							}
							else {
								throw new UnsupportedOperationException("What is "+arg.getClass().getName());
							}
							first2 = false;
						}
						System.out.print(">");
					}
					
				}
				else if (type instanceof Class) {
					System.out.println(((Class<?>)type).getSimpleName());
				}
				else {
					throw new UnsupportedOperationException("What is "+type.getClass().getName());
				}
				//System.out.print(type);
				first = false;
			}
			System.out.println(")");
		}
		for (Method method: typeAdaptorClass.getMethods()) {
			if (method.getName().equals("toObjectValue") || method.getName().equals("toDBvValue")) {
				System.out.println("  "+descriptionOf(method));
			}
		}
		System.out.println();
	}
	
	protected static String descriptionOf(Method method) {
		StringBuilder buf = new StringBuilder();
		buf.append(method.getReturnType() == null ? null : method.getReturnType().getSimpleName());
		buf.append(" ");
		buf.append(method.getName());
		buf.append("(");
		boolean first = true;
		for (Class<?> paramType: method.getParameterTypes()) {
			if (!first) buf.append(",");
			buf.append(paramType.getSimpleName());
			first = false;
		}
		if (method.isVarArgs()) {
			buf.append("..."); // applies to last parameter
		}
		buf.append(")");
		
		if (method.isSynthetic() || method.isBridge()) {
			buf.append("    {");
			if (method.isSynthetic() && method.isBridge()) {
				buf.append("synthetic,bridge");
			}
			else if (method.isSynthetic()) {
				buf.append("synthetic");
			}
			else if (method.isBridge()) {
				buf.append("bridge");
			}
			buf.append("}");
		}
		
		return buf.toString();
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

	public static interface MyGenericNumberInterfaceAdaptor<T extends Number,D extends DBNumber> extends DBTypeAdaptor<T,D> {
	}
	
	public static class MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods implements DBTypeAdaptor<Integer, DBInteger> {
		public Integer toObjectValue(DBInteger dbvValue) {
			throw new UnsupportedOperationException("toObjectValue(DBInteger): Integer");
		}

		public Number toObjectValue(DBNumber dbvValue) {
			throw new UnsupportedOperationException("toObjectValue(DBNumber): Number");
		}

		public DBInteger toDBvValue(Integer objectValue) {
			throw new UnsupportedOperationException("toDBvValue(Integer): DBInteger");
		}
		
		public DBNumber toDBvValue(Number objectValue) {
			throw new UnsupportedOperationException("toDBvValue(Number): DBNumber");
		}
	}

	public static class MyNumberDBNumberAdaptorWithIntegerDBIntegerMethods implements DBTypeAdaptor<Number, DBNumber> {
		public Integer toObjectValue(DBInteger dbvValue) {
			throw new UnsupportedOperationException("toObjectValue(DBInteger): Integer");
		}

		public Number toObjectValue(DBNumber dbvValue) {
			throw new UnsupportedOperationException("toObjectValue(DBNumber): Number");
		}

		public DBInteger toDBvValue(Integer objectValue) {
			throw new UnsupportedOperationException("toDBvValue(Integer): DBInteger");
		}
		
		public DBNumber toDBvValue(Number objectValue) {
			throw new UnsupportedOperationException("toDBvValue(Number): DBNumber");
		}
	}
}
