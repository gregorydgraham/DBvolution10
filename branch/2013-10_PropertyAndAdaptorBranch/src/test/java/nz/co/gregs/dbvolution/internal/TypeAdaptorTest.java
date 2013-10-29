package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("serial")
public class TypeAdaptorTest {
	private DBDatabase db;
	
	@Before
	public void setup() throws SQLException {
		this.db = new H2MemoryDB("dbvolutionTest","","", false);
		
		// create tables and add standard records
		db.createTable(new CustomerWithDBInteger());
		
//		System.out.println("Threads:");
//		for(Thread thread: Thread.getAllStackTraces().keySet()) {
//			System.out.println("  "+thread.getId()+": "+thread.getName());
//		}
		
		CustomerWithDBInteger c = new CustomerWithDBInteger();
		c.uid.setValue(23);
		c.year.setValue(2013);
		db.insert(c);

		c = new CustomerWithDBInteger();
		c.uid.setValue(22);
		c.year.setValue(2012);
		db.insert(c);
	}
	
	@After
	public void tearDown() throws Exception {
		db.dropTable(new CustomerWithDBInteger());
        db.dropDatabase();
	}
	
	private void assertDbSetup() throws SQLException {
		CustomerWithDBInteger q = new CustomerWithDBInteger();
		q.uid.permittedValues(23);
		List<CustomerWithDBInteger> rows = db.get(q);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).uid.intValue(), is(23));
		assertThat(rows.get(0).year.intValue(), is(2013));

		q = new CustomerWithDBInteger();
		q.uid.permittedValues(22);
		rows = db.get(q);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).uid.intValue(), is(22));
		assertThat(rows.get(0).year.intValue(), is(2012));
	}
	
//	@Test
//	public void createDatabaseFromSimpleTestClasses() throws SQLException {
//		db.createTable(new CustomerWithDBInteger());
//		
//		CustomerWithDBInteger c = new CustomerWithDBInteger();
//		c.uid.setValue(23);
//		db.insert(c);
//		
//		CustomerWithDBInteger q = new CustomerWithDBInteger();
//		q.uid.permittedValues(23);
//		List<CustomerWithDBInteger> rows = db.get(q);
//		assertThat(rows.size(), is(1));
//		assertThat(rows.get(0).uid.intValue(), is(23));
//	}

	@Test
	public void queriesOnDBIntegerGivenStringIntegerTypeAdaptor() throws SQLException {
		assertDbSetup();
		CustomerWithStringIntegerTypeAdaptor query = new CustomerWithStringIntegerTypeAdaptor();
		query.uid.permittedValues(23);
		
		List<CustomerWithStringIntegerTypeAdaptor> rows = db.get(query);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).uid.intValue(), is(23));
		assertThat(rows.get(0).year, is("2013"));
	}
	
	@Test
	public void queriesOnNonNullStringGivenStringIntegerTypeAdaptor() throws SQLException {
		assertDbSetup();
		CustomerWithStringIntegerTypeAdaptor query = new CustomerWithStringIntegerTypeAdaptor();
		query.year = "2013";
		
		List<CustomerWithStringIntegerTypeAdaptor> rows = db.get(query);
		assertThat(rows.size(), is(1));
		assertThat(rows.get(0).uid.intValue(), is(23));
		assertThat(rows.get(0).year, is("2013"));
	}
	
	@Test
	public void insertsGivenStringIntegerTypeAdaptor() throws SQLException {
		CustomerWithStringIntegerTypeAdaptor row = new CustomerWithStringIntegerTypeAdaptor();
		row.uid.setValue(50);
		row.year = "2050";
		db.insert(row);
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
		public static class MyTypeAdaptor implements DBTypeAdaptor<String, Integer> {
			public String fromDatabaseValue(Integer dbvValue) {
				return (dbvValue == null) ? null : dbvValue.toString();
			}

			public Integer toDatabaseValue(String objectValue) {
				return (objectValue == null) ? null : Integer.parseInt(objectValue);
			}
		}
		
		@DBPrimaryKey
		@DBColumn
		public DBInteger uid = new DBInteger();
		
		@DBColumn
		@DBAdaptType(adaptor=MyTypeAdaptor.class, type=DBInteger.class)
		public String year;
	}
}
