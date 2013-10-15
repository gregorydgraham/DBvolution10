package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

import org.junit.Test;

public class InterfaceInfoTest {
	@Test
	public void recognisesImplementationGivenStandardInputs() {
		InterfaceInfo info = new InterfaceInfo(MyInterface.class, SimpleIntegerDBIntegerImpl.class);
		assertThat(info.isInterfaceImplementedByImplementation(), is(true));
	}

	@Test
	public void getsCorrectMethodGivenStandardInputs() {
		InterfaceInfo info = new InterfaceInfo(MyInterface.class, SimpleIntegerDBIntegerImpl.class);
		Method method = info.getImplementationMethod("toObjectValue", QueryableDatatype.class);
		assertThat(method, is(not(nullValue())));
		assertThat((Object)method.getReturnType(), is((Object)Integer.class));
		assertThat(Arrays.asList(method.getParameterTypes()), contains(is((Object)DBInteger.class)));
	}

	@Test
	public void getsCorrectMethodGivenStandardInputs2() {
		InterfaceInfo info = new InterfaceInfo(MyInterface.class, SimpleIntegerDBIntegerImpl.class);
		Method method = info.getImplementationMethod("toDBvValue", Object.class);
		assertThat(method, is(not(nullValue())));
		assertThat((Object)method.getReturnType(), is((Object)DBInteger.class));
		assertThat(Arrays.asList(method.getParameterTypes()), contains(is((Object)Integer.class)));
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

	public static class MyIntegerDBIntegerAdaptorWithNumberDBNumberMethods implements MyInterface<Integer, DBInteger> {
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

	public static class MyNumberDBNumberAdaptorWithIntegerDBIntegerMethods implements MyInterface<Number, DBNumber> {
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

	public class ConcretePartialImplementationOfConcreteType extends AbstractPartialImplementationWithConcreteType {
		@Override
		public DBNumber toDBvValue(Number objectValue) {
			return null;
		}
	}

	public class ConcretePartialImplementationOfWildcardType extends AbstractPartialImplementationWithWildcardType<Integer, DBInteger> {
		@Override
		public DBInteger toDBvValue(Integer objectValue) {
			return null;
		}
	}
}
