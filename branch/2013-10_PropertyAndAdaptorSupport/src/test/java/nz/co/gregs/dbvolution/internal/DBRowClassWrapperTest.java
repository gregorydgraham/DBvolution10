package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
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
				new DBDatabase(), new DBRowClassWrapperFactory()),
				is("uid_2"));
	}
	
	@DBTableName("table1")
	public static class MyTable1 {
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
	public static class MyTable2 {
		@DBPrimaryKey
		@DBColumn("uid_2")
		public DBInteger uid = new DBInteger();
	}
}
