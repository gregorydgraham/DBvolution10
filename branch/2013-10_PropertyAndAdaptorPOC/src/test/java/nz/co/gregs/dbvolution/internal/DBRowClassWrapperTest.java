package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.definitions.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;

import org.junit.Test;

public class DBRowClassWrapperTest {
	
	@Test
	public void getsPrimaryKeyPropertiesGivenOnePrimaryKeyColumn() {
		DBRowClassWrapper classWrapper = new DBRowClassWrapper(MyTable1.class);
		assertThat(classWrapper.primaryKey(), is(not(nullValue())));
		assertThat(classWrapper.primaryKey().size(), is(1));
		assertThat(classWrapper.primaryKey().get(0).columnName(), is("uid"));
	}
	
	@Test
	public void getsPrimaryKeyPropertiesGivenTwoPrimaryKeyColumns() {
		@DBTableName("table2")
		class TestClass extends DBRow {
			@DBPrimaryKey
			@DBColumn("uid_2")
			public DBInteger uid = new DBInteger();

			@DBPrimaryKey
			@DBColumn
			public DBInteger type = new DBInteger();
		}
		
		DBRowClassWrapper classWrapper = new DBRowClassWrapper(TestClass.class);
		assertThat(classWrapper.primaryKey(), is(not(nullValue())));
		assertThat(classWrapper.primaryKey().size(), is(2));
		assertThat(classWrapper.primaryKey().get(0).columnName(), is("uid_2"));
		assertThat(classWrapper.primaryKey().get(1).columnName(), is("type"));
	}
	
	@Test
	public void getsProperties() {
		DBRowClassWrapper classAdaptor = new DBRowClassWrapper(MyTable1.class);
		assertThat(classAdaptor.getProperties().size(), is(3));
	}
	
	@Test
	public void getsForeignKeyReferencedTableName() {
		DBRowClassWrapper classWrapper = new DBRowClassWrapper(MyTable1.class);
		assertThat(classWrapper.getPropertyByName("fkTable2").referencedTableName(), is("table2"));
	}

	@Test
	public void getsForeignKeyReferencedColumnName() {
		DBRowClassWrapper classWrapper = new DBRowClassWrapper(MyTable1.class);
		assertThat(classWrapper.getPropertyByName("fkTable2").referencedColumnName(
				new DBDatabase(), new DBRowWrapperFactory()),
				is("uid_2"));
	}
	
	@DBTableName("table1")
	public static class MyTable1 extends DBRow {
		@DBPrimaryKey
		@DBColumn
		public DBInteger uid = new DBInteger();
		
		@DBColumn("table_text")
		public DBString text = new DBString();
		
		@DBColumn
		@DBForeignKey(value=MyTable2.class)
		public DBInteger fkTable2 = new DBInteger();
	}
	
	@DBTableName("table2")
	public static class MyTable2 extends DBRow {
		@DBPrimaryKey
		@DBColumn("uid_2")
		public DBInteger uid = new DBInteger();
	}
}
