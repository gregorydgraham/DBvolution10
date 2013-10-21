package nz.co.gregs.dbvolution.internal;

import java.util.List;

import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

import org.junit.Test;

public class InterfaceImplementationUnderstandingTests {
	
	@Test
	public void printAbstractClasses() {
		TypeTestUtils.describeClass(AbstractPartialImplementationWithConcreteType.class);
		TypeTestUtils.describeClass(AbstractPartialImplementationWithWildcardType.class);
	}

	@Test
	public void printConcreteClasses() {
		TypeTestUtils.describeClass(ConcretePartialImplementationOfConcreteType.class);
		TypeTestUtils.describeClass(ConcretePartialImplementationOfWildcardType.class);
		TypeTestUtils.describeClass(ConcretePartialImplementationOfWildcardTypeWithAgreeingInterface.class);
	}

	@Test
	public void printExtraLevelOfAbstractionClasses() {
		TypeTestUtils.describeClass(AbstractPartialReImplementationOfWildcardTypeWithWildcardType.class);
		TypeTestUtils.describeClass(ConcretePartialReImplementationOfWildcardTypeWithWildcardType.class);
	}
	
	@Test
	public void printInterfaces() {
		TypeTestUtils.describeClass(MyInterface.class);
		TypeTestUtils.describeClass(List.class);
	}

	@Test
	public void printWildcardClasses() {
		TypeTestUtils.describeClass(WildWithSuper.class);
	}
	
	public class WildWithSuper<T extends Object> {
		public void simpleMethod(T foo) {
			throw new UnsupportedOperationException();
		}
		
		public void acceptSingleSuper(List<? super Number> param) {
			throw new UnsupportedOperationException();
		}

		public void acceptReferencingSuper(List<? super T> param) {
		}
	}
	
	public interface MyInterface<T, Q extends QueryableDatatype> {
		public T toObjectValue(Q dbvValue);

		public Q toDBvValue(T objectValue);
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

	public class ConcretePartialImplementationOfWildcardTypeWithAgreeingInterface
			extends AbstractPartialImplementationWithWildcardType<Integer, DBInteger>
			implements MyInterface<Integer, DBInteger> {
		@Override
		public DBInteger toDBvValue(Integer objectValue) {
			return null;
		}
	}


	public abstract class AbstractPartialReImplementationOfWildcardTypeWithWildcardType<I extends Integer> extends AbstractPartialImplementationWithWildcardType<I, DBInteger> {
		@Override
		public DBInteger toDBvValue(I objectValue) {
			return null;
		}
	}
	
	public class ConcretePartialReImplementationOfWildcardTypeWithWildcardType extends AbstractPartialReImplementationOfWildcardTypeWithWildcardType<Integer> {
	}
}
