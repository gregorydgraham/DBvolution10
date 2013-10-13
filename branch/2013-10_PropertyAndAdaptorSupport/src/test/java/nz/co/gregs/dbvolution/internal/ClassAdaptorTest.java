package nz.co.gregs.dbvolution.internal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;

import org.junit.Test;

public class ClassAdaptorTest {
	
	@Test
	public void getsPrimaryKeyPropertiesGivenOnePrimaryKeyColumn() {
		ClassAdaptor classAdaptor = new ClassAdaptor(MyTable1.class);
		assertThat(classAdaptor.primaryKey(), is(not(nullValue())));
		assertThat(classAdaptor.primaryKey().size(), is(1));
		assertThat(classAdaptor.primaryKey().get(0).columnName(), is("uid"));
	}
	
	@Test
	public void getsProperties() {
		ClassAdaptor classAdaptor = new ClassAdaptor(MyTable1.class);
		assertThat(classAdaptor.getProperties().size(), is(3));
	}
	
	@Test
	public void getsForeignKeyReferencedTableName() {
		ClassAdaptor classAdaptor = new ClassAdaptor(MyTable1.class);
		assertThat(classAdaptor.getPropertyByName("fkTable2").referencedTableName(), is("table2"));
	}

	@Test
	public void getsForeignKeyReferencedColumnName() {
		ClassAdaptor classAdaptor = new ClassAdaptor(MyTable1.class);
		assertThat(classAdaptor.getPropertyByName("fkTable2").referencedColumnName(
				new DBDefinition(), new ClassAdaptorCache()),
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
