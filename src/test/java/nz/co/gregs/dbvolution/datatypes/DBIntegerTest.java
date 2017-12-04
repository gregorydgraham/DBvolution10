package nz.co.gregs.dbvolution.datatypes;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;

import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.generic.AbstractTest;

import org.junit.Test;

public class DBIntegerTest extends AbstractTest {

	public DBIntegerTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

//	@Ignore("DBOperator and QueryableDatatype overload instead of override Object.equals()")
	@Test
	public void equalGivenSameLiteralValue() {
		DBInteger int1 = new DBInteger();
		DBInteger int2 = new DBInteger();
		int1.setValue(100);
		int2.setValue(100L);
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

//	@Ignore("DBOperator and QueryableDatatype overload instead of override Object.equals()")
	@Test
	public void equalGivenSameValuesFromDifferentSources() throws SQLException, UnexpectedNumberOfRowsException {
		DBInteger int1 = new DBInteger();
		int1.setValue(1);

		Marque marqueEx = new Marque();
		marqueEx.uidMarque.permittedValues(1);
		Marque result = database.getDBTable(marqueEx).getOnlyRow();
		DBInteger int2 = result.uidMarque;

		assertThat(int1.equals(int2), is(true));
	}

//	@Ignore("DBOperator and QueryableDatatype overload instead of override Object.equals()")
	@Test(expected = UnexpectedNumberOfRowsException.class)
	public void nonExistentValueAndGetOnlyThrowsUnexpectedNumberOfRows() throws SQLException, UnexpectedNumberOfRowsException {
		DBInteger int1 = new DBInteger();
		int1.setValue(1);

		Marque marqueEx = new Marque();
		marqueEx.uidMarque.permittedValues(-1);
		Marque result = database.getDBTable(marqueEx).getOnlyRow();
		DBInteger int2 = result.uidMarque;

		assertThat(int1.equals(int2), is(true));
	}

	@Test
	public void notEqualGivenDifferentValuesFromDifferentSources() throws SQLException {
		DBInteger int1 = new DBInteger();
		int1.setValue(2);

		Marque marqueEx = new Marque();
		marqueEx.uidMarque.permittedValues(1);
		List<Marque> results = database.get(marqueEx);
		DBInteger int2 = results.get(0).uidMarque;

		assertThat(int1.equals(int2), is(false));
	}
}
