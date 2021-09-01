package nz.co.gregs.dbvolution.internal.properties;

import java.sql.SQLException;
import static org.hamcrest.Matchers.*;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import static org.hamcrest.MatcherAssert.assertThat;


import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("warnings")
public class DBRowClassWrapperTest {

//	private static DBDatabase database;
	@BeforeClass
	public static void setup() throws SQLException {
//		database = new H2MemoryDB("dbvolutionTest", "", "", false);
	}

	@Test
	public void getsPrimaryKeyPropertiesGivenOnePrimaryKeyColumn() {
		var classWrapper = new RowDefinitionClassWrapper<>(MyTable1.class);
		assertThat(classWrapper.primaryKeyDefinitions()[0], is(not(nullValue())));
		assertThat(classWrapper.primaryKeyDefinitions()[0].getColumnName(), is("uid"));
	}

	public void errorsWhenConstructingGivenTwoPrimaryKeyColumns() {
		@DBTableName("table1")
		class TestClass extends DBRow {

			private static final long serialVersionUID = 1L;

			@DBPrimaryKey
			@DBColumn
			public DBInteger uid = new DBInteger();
			@DBPrimaryKey
			@DBColumn("table_text")
			public DBString text = new DBString();
			@DBColumn
			@DBForeignKey(value = MyTable2.class)
			public DBInteger fkTable2 = new DBInteger();
		}

		var rowDefinitionClassWrapper = new RowDefinitionClassWrapper<>(TestClass.class);
		assertThat(rowDefinitionClassWrapper, notNullValue());
	}

//	@Test
//	public void getsPrimaryKeyPropertiesGivenTwoPrimaryKeyColumns() {
//		@DBTableName("table2")
//		class TestClass extends DBRow {
//			@DBPrimaryKey
//			@DBColumn("uid_2")
//			public DBInteger uid = new DBInteger();
//
//			@DBPrimaryKey
//			@DBColumn
//			public DBInteger type = new DBInteger();
//		}
//		
//		RowDefinitionClassWrapper classWrapper = new RowDefinitionClassWrapper(TestClass.class);
//		assertThat(classWrapper.primaryKey(), is(not(nullValue())));
//		assertThat(classWrapper.primaryKey().size(), is(2));
//		assertThat(classWrapper.primaryKey().get(0).getColumnName(), is("uid_2"));
//		assertThat(classWrapper.primaryKey().get(1).getColumnName(), is("type"));
//	}
	@Test
	public void getsProperties() {
		var classAdaptor = new RowDefinitionClassWrapper<>(MyTable1.class);
		assertThat(classAdaptor.getColumnPropertyDefinitions().size(), is(3));
	}

	@Test
	public void getsForeignKeyReferencedTableName() {
		var classWrapper = new RowDefinitionClassWrapper<>(MyTable1.class);
		assertThat(classWrapper.getPropertyDefinitionByName("fkTable2").referencedTableName(), is("table2"));
	}

	@Test
	public void getsForeignKeyReferencedColumnName() {
		var classWrapper = new RowDefinitionClassWrapper<>(MyTable1.class);
		assertThat(classWrapper.getPropertyDefinitionByName("fkTable2").referencedColumnName(), is("uid_2"));
	}

	@SuppressWarnings("serial")
	@DBTableName("table1")
	public static class MyTable1 extends DBRow {

		@DBPrimaryKey
		@DBColumn
		public DBInteger uid = new DBInteger();
		@DBColumn("table_text")
		public DBString text = new DBString();
		@DBColumn
		@DBForeignKey(value = MyTable2.class)
		public DBInteger fkTable2 = new DBInteger();
	}

	@SuppressWarnings("serial")
	@DBTableName("table2")
	public static class MyTable2 extends DBRow {

		@DBPrimaryKey
		@DBColumn("uid_2")
		public DBInteger uid = new DBInteger();
	}
}
