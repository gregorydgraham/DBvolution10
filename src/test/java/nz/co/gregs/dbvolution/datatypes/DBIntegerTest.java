package nz.co.gregs.dbvolution.datatypes;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

public class DBIntegerTest {
	@Ignore("DBOperator and QueryableDatatype overload instead of override Object.equals()")
	@Test
	public void equalGivenSameLiteralValue() {
		DBInteger int1 = new DBInteger();
		DBInteger int2 = new DBInteger();
		int1.setValue(100);
		int2.setValue(100);
		assertThat(int1.equals(int2), is(true));
	}

	@Test
	public void notEqualGivenDifferentLiteralValues() {
		DBInteger int1 = new DBInteger();
		DBInteger int2 = new DBInteger();
		int1.setValue(100);
		int2.setValue(101);
		assertThat(int1.equals(int2), is(false));
	}
}
