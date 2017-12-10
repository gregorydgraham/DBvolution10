package nz.co.gregs.dbvolution.internal.properties;

import static nz.co.gregs.dbvolution.internal.properties.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Focuses on end-to-end functionality with regards to type adaptors against an
 * actual database. More detailed low-level unit testing of Type Adaptor
 * functionality is in {@link PropertyTypeHandlerTest}.
 */
@SuppressWarnings("serial")
public class TypeAdaptorTest {

	private DBDatabase db;

	@Before
	public void setup() throws SQLException {
		this.db = new H2MemoryDB("dbvolutionTest", "", "", false);

		db.preventDroppingOfTables(false);
		db.dropTableNoExceptions(new CustomerWithDBInteger());
		// create tables and add standard records
		db.createTable(new CustomerWithDBInteger());

		CustomerWithDBInteger c = new CustomerWithDBInteger();
		c.uid.setValue(23);
		c.year.setValue(2013);
		db.insert(c);

		c = new CustomerWithDBInteger();
		c.uid.setValue(22);
		c.year.setValue(2012);
		db.insert(c);

//		this.db.setPrintSQLBeforeExecuting(true);
	}

	@After
	public void tearDown() throws Exception {
		db.setPrintSQLBeforeExecuting(false);
		db.preventDroppingOfTables(false);
		db.dropTable(new CustomerWithDBInteger());
		try {
			db.preventDroppingOfTables(false);
			db.preventDroppingOfDatabases(false);
			db.dropDatabase(true);
		} catch (UnsupportedOperationException ex) {
			;
		}
	}

	@Test
	public void queriesOnDBIntegerGivenDBInteger() throws SQLException {
		CustomerWithDBInteger query = new CustomerWithDBInteger();
		query.uid.permittedValues(23);

		List<CustomerWithDBInteger> rows = db.get(query);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).uid.getValue().intValue(), is(23));
		assertThat(rows.get(0).year.getValue().intValue(), is(2013));
	}

	@Test
	public void queriesOnDBIntegerGivenStringIntegerTypeAdaptor() throws SQLException {
		CustomerWithStringIntegerTypeAdaptor query = new CustomerWithStringIntegerTypeAdaptor();
		query.uid.permittedValues(23);

		List<CustomerWithStringIntegerTypeAdaptor> rows = db.get(query);
		assertThat(rows.size(), is(1));
	}

	@Test
	public void queriesOnNonNullStringGivenStringIntegerTypeAdaptor() throws SQLException {
		CustomerWithStringIntegerTypeAdaptor query = new CustomerWithStringIntegerTypeAdaptor();
		query.year = "2013";

		List<CustomerWithStringIntegerTypeAdaptor> rows = db.get(query);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).uid.getValue().intValue(), is(23));
		assertThat(rows.get(0).year, is("2013"));
	}

	@Test
	public void queriesOnStringRangeGivenStringIntegerTypeAdaptor() throws SQLException {
		CustomerWithDBStringIntegerTypeAdaptor query = new CustomerWithDBStringIntegerTypeAdaptor();
		query.year.permittedRange("25", "3000");

		List<CustomerWithDBStringIntegerTypeAdaptor> rows = db.get(query);
		List<String> whereClauses = query.getWhereClausesWithoutAliases(db.getDefinition());
		String allClauses = "";
		for (String clause : whereClauses) {
			allClauses += " and " + clause;
		}
		assertThat(allClauses, matchesRegex(".*>.*25.*<=.*3000.*"));
		assertThat(rows.size(), is(2));
	}

	@Test
	public void queriesWithoutFilteringOnNullSimpleFieldGivenTypeAdaptor() throws SQLException {
		CustomerWithStringIntegerTypeAdaptor exemplar = new CustomerWithStringIntegerTypeAdaptor();
		exemplar.uid.clear();
		exemplar.year = null;

		DBQuery query = db.getDBQuery(exemplar);
//        DBQuery query = DBQuery.getInstance(db, exemplar);
		query.setBlankQueryAllowed(true);

		List<CustomerWithStringIntegerTypeAdaptor> rows = query.getAllInstancesOf(exemplar);
		assertThat(rows.size(), is(2));
	}

	@Test
	public void populatesStringWhenQueryingOnDBIntegerGivenStringIntegerTypeAdaptor() throws SQLException {
		CustomerWithStringIntegerTypeAdaptor query = new CustomerWithStringIntegerTypeAdaptor();
		query.uid.permittedValues(23);

		List<CustomerWithStringIntegerTypeAdaptor> rows = db.get(query);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).uid.getValue().intValue(), is(23));
		assertThat(rows.get(0).year, is("2013"));
	}

	@Test
	public void insertsGivenStringIntegerTypeAdaptor() throws SQLException {
		CustomerWithStringIntegerTypeAdaptor row = new CustomerWithStringIntegerTypeAdaptor();
		row.uid.setValue(50);
		row.year = "2050";
		db.insert(row);

		CustomerWithDBInteger q = new CustomerWithDBInteger();
		q.uid.permittedValues(50);
		List<CustomerWithDBInteger> rows = db.get(q);

		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).uid.getValue().intValue(), is(50));
		assertThat(rows.get(0).year.getValue().intValue(), is(2050));
	}

	// base-case "Customer": DBInteger uid (PK), DBInteger year
	@DBTableName("Customer")
	public static class CustomerWithDBInteger extends DBRow {

		@DBColumn
		public DBInteger uid = new DBInteger();

		@DBColumn
		public DBInteger year = new DBInteger();
	}

	@DBTableName("Customer")
	public static class CustomerWithStringIntegerTypeAdaptor extends DBRow {

		public static class MyTypeAdaptor implements DBTypeAdaptor<String, Long> {

			@Override
			public String fromDatabaseValue(Long dbvValue) {
				return (dbvValue == null) ? null : dbvValue.toString();
			}

			@Override
			public Long toDatabaseValue(String objectValue) {
				return (objectValue == null) ? null : ("".equals(objectValue)) ? null : Long.parseLong(objectValue);
			}
		}

		@DBPrimaryKey
		@DBColumn
		public DBInteger uid = new DBInteger();

		@DBColumn
		@DBAdaptType(value = MyTypeAdaptor.class, type = DBInteger.class)
		public String year = new String();
	}

	@DBTableName("Customer")
	public static class CustomerWithDBStringIntegerTypeAdaptor extends DBRow {

		public static class MyTypeAdaptor implements DBTypeAdaptor<String, Long> {

			@Override
			public String fromDatabaseValue(Long dbvValue) {
				return (dbvValue == null) ? null : dbvValue.toString();
			}

			@Override
			public Long toDatabaseValue(String objectValue) {
				return (objectValue == null) ? null : Long.parseLong(objectValue);
			}
		}

		@DBPrimaryKey
		@DBColumn
		public DBInteger uid = new DBInteger();

		@DBColumn
		@DBAdaptType(value = MyTypeAdaptor.class, type = DBInteger.class)
		public DBString year = new DBString();
	}
}
