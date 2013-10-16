package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.InterfaceInfo.ParameterBounds;

import org.junit.Test;

public class InterfaceInfoTest {
	@Test
	public void recognisesImplementationGivenStandardInputs() {
		InterfaceInfo info = new InterfaceInfo(MyInterface.class,
				SimpleIntegerDBIntegerImpl.class);
		assertThat(info.isInterfaceImplementedByImplementation(), is(true));
	}
	
	@Test
	public void getsBoundsGivenDirectImplementationUsingClassTypeArguments() {
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class,
				SimpleIntegerDBIntegerImpl.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperType(), is((Object)Integer.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)DBInteger.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}
	
	@Test
	public void getsBoundsGivenDirectImplementationUsingTypeVariableArguments() {
		class GenericIntegerDBIntegerImpl<A extends Number, B extends DBNumber> implements MyInterface<A, B> {
			public A toObjectValue(B dbvValue) {
				return null;
			}
			public B toDBvValue(A objectValue) {
				return null;
			}
		}
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class,
				GenericIntegerDBIntegerImpl.class);
		System.out.println(Arrays.toString(bounds));
		
		assertThat(bounds[0].upperType(), is((Object)Number.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)DBNumber.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}
	
	@Test
	public void getsBoundsGivenDirectImplementationUsingClassTypeArgumentsAndExtraneousMethods() {
		class MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods implements MyInterface<Integer, DBInteger> {
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
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class,
				MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperType(), is((Object)Integer.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)DBInteger.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}
	
	@Test
	public void getsBoundsGivenIndirectImplementationUsingClassTypeArguments() {
		class ConcretePartialImplementationOfConcreteType
				extends AbstractPartialImplementationWithConcreteType {
			public DBNumber toDBvValue(Number objectValue) {
				return null;
			}
		}
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class,
				ConcretePartialImplementationOfConcreteType.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperType(), is((Object)Number.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)DBNumber.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}
	
	@Test
	public void getsBoundsGivenIndirectImplementationUsingConcretizedWildcardTypeArguments() {
		class ConcretePartialImplementationOfWildcardType
				extends AbstractPartialImplementationWithWildcardType<Integer, DBInteger> {
			public DBInteger toDBvValue(Integer objectValue) {
				return null;
			}
		}
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class,
				ConcretePartialImplementationOfWildcardType.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperType(), is((Object)Integer.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)DBInteger.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}
	
	@Test
	public void getsBoundsGivenDoublyIndirectImplementationAndScatteredClassTypeArguments() {
		abstract class AbstractPartialReImplementationOfWildcardTypeWithWildcardType<I extends Integer>
				extends AbstractPartialImplementationWithWildcardType<I, DBInteger> {
			public DBInteger toDBvValue(I objectValue) {
				return null;
			}
		}
		
		class ConcretePartialReImplementationOfWildcardTypeWithWildcardType
				extends AbstractPartialReImplementationOfWildcardTypeWithWildcardType<Integer> {
		}
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class,
				ConcretePartialReImplementationOfWildcardTypeWithWildcardType.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperType(), is((Object)Integer.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)DBInteger.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}
	
	@Test
	public void getsDefaultBoundsGivenItself() {
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class, MyInterface.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperType(), is((Object)Object.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)QueryableDatatype.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}

	@Test
	public void getsDefaultBoundsGivenOnClassWhenUsingConvenienceMethod() {
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperType(), is((Object)Object.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)QueryableDatatype.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}
	
	@Test
	public void getsDefaultBoundsOnObjectClass() {
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(Object.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds.length, is(0));
	}

	@Test
	public void getsDefaultBoundsOnClassClass() {
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(Class.class);
		System.out.println(Arrays.toString(bounds));
		
		assertThat(bounds[0].upperType(), is((Object)Object.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
	}

	@Test
	public void getsDefaultBoundsOnEnumClass() {
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(Enum.class);
		System.out.println(Arrays.toString(bounds));
		
		assertThat(bounds[0].upperClass(), is((Object)Enum.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
	}
	
	@Test
	public void getsNullBoundsGivenNonImplementation() {
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class, Class.class);
		assertThat(bounds, is(nullValue()));
	}

	@Test
	public void getsDefaultBoundsGivenNonSpecifiedTypeArguments() {
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MyInterface.class,
				new MyInterface(){
					public Object toObjectValue(QueryableDatatype dbvValue) {
						return null;
					}
					public QueryableDatatype toDBvValue(Object objectValue) {
						return null;
					}
			}.getClass());
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperType(), is((Object)Object.class));
		assertThat(bounds[0].lowerType(), is(nullValue()));
		
		assertThat(bounds[1].upperType(), is((Object)QueryableDatatype.class));
		assertThat(bounds[1].lowerType(), is(nullValue()));
	}

	@Test
	public void getsBoundsGivenInterfaceTypeWithMultipleBoundingTypes() {
		class MultiBoundedInterface<T extends Serializable & Map> {
		}
		
		class MySubclass<P extends Serializable & Map> extends MultiBoundedInterface<P> {
		}
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(MultiBoundedInterface.class, MySubclass.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(Arrays.asList(bounds[0].upperTypes()), contains((Object)Serializable.class, (Object)Map.class));
		assertThat(bounds[0].lowerTypes(), is(nullValue()));
	}

	@Test
	public void getsBoundsGivenInterfaceTypeWithSimpleParameterizedArgument() {
		class ParamaterizedArgumentInterface<T extends Map<?,?>> {
		}
		
		class MySubclass extends ParamaterizedArgumentInterface<HashMap<Object, Number>> {
		}
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(ParamaterizedArgumentInterface.class,
				MySubclass.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperClass(), is((Object)HashMap.class));
		assertThat(bounds[0].lowerClass(), is(nullValue()));
	}

	@Test
	public void getsBoundsGivenInterfaceTypeWithSimpleParameterizedArgument2() {
		class ParamaterizedArgumentInterface<T extends Map<?,?>> {
		}
		
		class MySubclass extends ParamaterizedArgumentInterface {
		}
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(ParamaterizedArgumentInterface.class,
				MySubclass.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperClass(), is((Object)Map.class));
		assertThat(bounds[0].lowerClass(), is(nullValue()));
	}

	@Test
	public void getsBoundsGivenInterfaceTypeWithRecursiveParameterizedArgument() {
		class ParamaterizedArgumentInterface<E extends Enum<E>> {
		}
		
		class MySubclass extends ParamaterizedArgumentInterface {
		}
		
		ParameterBounds[] bounds = InterfaceInfo.getParameterBounds(ParamaterizedArgumentInterface.class,
				MySubclass.class);
		System.out.println(Arrays.toString(bounds));
		assertThat(bounds[0].upperClass(), is((Object)Enum.class));
		assertThat(bounds[0].lowerClass(), is(nullValue()));
	}
	
	@Test
	public void quietlyRecognisesNotImplementation() {
		InterfaceInfo info = new InterfaceInfo(List.class, SimpleIntegerDBIntegerImpl.class);
		assertThat(info.isInterfaceImplementedByImplementation(), is(false));
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorsWhenInterfaceNotAnInterface() {
		new InterfaceInfo(ArrayList.class, SimpleIntegerDBIntegerImpl.class);
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorsWhenImplIsAbstract() {
		new InterfaceInfo(MyInterface.class, AbstractPartialImplementationWithWildcardType.class);
	}

	@Test(expected=IllegalArgumentException.class)
	public void errorsWhenImplIsInterface() {
		new InterfaceInfo(MyInterface.class, List.class);
	}

	@Test
	public void acceptsAnonymousImpl() {
		new InterfaceInfo(MyInterface.class, new MyInterface<Object,QueryableDatatype>(){
			public Object toObjectValue(QueryableDatatype dbvValue) {
				return null;
			}

			public QueryableDatatype toDBvValue(Object objectValue) {
				return null;
			}}.getClass());
	}
	
	public interface MyInterface<T, Q extends QueryableDatatype> {
		public T toObjectValue(Q dbvValue);

		public Q toDBvValue(T objectValue);
	}	
	
	public static class SimpleIntegerDBIntegerImpl implements MyInterface<Integer, DBInteger> {
		public Integer toObjectValue(DBInteger dbvValue) {
			return null;
		}

		@Override
		public DBInteger toDBvValue(Integer objectValue) {
			return null;
		}
	}

//	public static class MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods implements MyInterface<Integer, DBInteger> {
//		@Override
//		public Integer toObjectValue(DBInteger dbvValue) {
//			throw new UnsupportedOperationException("toObjectValue(DBInteger): Integer");
//		}
//
//		//@Override
//		public Number toObjectValue(DBNumber dbvValue) {
//			throw new UnsupportedOperationException("toObjectValue(DBNumber): Number");
//		}
//
//		@Override
//		public DBInteger toDBvValue(Integer objectValue) {
//			throw new UnsupportedOperationException("toDBvValue(Integer): DBInteger");
//		}
//		
//		//@Override
//		public DBNumber toDBvValue(Number objectValue) {
//			throw new UnsupportedOperationException("toDBvValue(Number): DBNumber");
//		}
//	}

//	private static class MyNumberDBNumberAdaptorWithIntegerDBIntegerMethods implements MyInterface<Number, DBNumber> {
//		//@Override
//		public Integer toObjectValue(DBInteger dbvValue) {
//			throw new UnsupportedOperationException("toObjectValue(DBInteger): Integer");
//		}
//
//		@Override
//		public Number toObjectValue(DBNumber dbvValue) {
//			throw new UnsupportedOperationException("toObjectValue(DBNumber): Number");
//		}
//
//		//@Override
//		public DBInteger toDBvValue(Integer objectValue) {
//			throw new UnsupportedOperationException("toDBvValue(Integer): DBInteger");
//		}
//		
//		@Override
//		public DBNumber toDBvValue(Number objectValue) {
//			throw new UnsupportedOperationException("toDBvValue(Number): DBNumber");
//		}
//	}
	
	public abstract class AbstractPartialImplementationWithConcreteType implements MyInterface<Number, DBNumber> {
		@Override
		public Number toObjectValue(DBNumber dbvValue) {
			return null;
		}
	}

	public abstract class AbstractPartialImplementationWithWildcardType<T extends Number, Q extends DBNumber> implements MyInterface<T, Q> {
		@Override
		public T toObjectValue(Q dbvValue) {
			return null;
		}
	}

//	public class ConcretePartialImplementationOfConcreteType extends AbstractPartialImplementationWithConcreteType {
//		@Override
//		public DBNumber toDBvValue(Number objectValue) {
//			return null;
//		}
//	}

//	public class ConcretePartialImplementationOfWildcardType extends AbstractPartialImplementationWithWildcardType<Integer, DBInteger> {
//		@Override
//		public DBInteger toDBvValue(Integer objectValue) {
//			return null;
//		}
//	}
}
