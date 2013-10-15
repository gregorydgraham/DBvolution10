package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

import org.junit.Test;

public class InterfaceImplementationUnderstandingTests {
	
	@Test
	public void printAbstractConcreteMethods() {
		TypeTestUtils.describeClass(AbstractPartialImplementationWithConcreteType.class);
		TypeTestUtils.describeClass(AbstractPartialImplementationWithWildcardType.class);
		TypeTestUtils.describeClass(ConcretePartialImplementationOfConcreteType.class);
		TypeTestUtils.describeClass(ConcretePartialImplementationOfWildcardType.class);
		TypeTestUtils.describeClass(ConcretePartialImplementationOfWildcardType.class);
		TypeTestUtils.describeClass(MyInterface.class);
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
}
