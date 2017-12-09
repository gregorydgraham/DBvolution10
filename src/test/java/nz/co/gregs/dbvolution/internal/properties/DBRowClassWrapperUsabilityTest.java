package nz.co.gregs.dbvolution.internal.properties;

import java.sql.SQLException;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unused")
public class DBRowClassWrapperUsabilityTest {

	private final MyExampleTableClass obj = new MyExampleTableClass();
	private final RowDefinitionWrapperFactory factory = new RowDefinitionWrapperFactory();
	private static DBDatabase database;

	@BeforeClass
	public static void setup() throws SQLException {
		database = new H2MemoryDB("dbvolutionTest", "", "", false);
	}

	@Test
	public void easyToGetSpecificPropertyValueOnObjectWhenDoingInline() {
		QueryableDatatype<?> qdt = new RowDefinitionClassWrapper(MyExampleTableClass.class)
				.instanceWrapperFor(obj)
				.getPropertyByColumn(database, "column1")
				.getQueryableDatatype();
	}

	@Test
	public void easyToGetSpecificPropertyValueOnObjectWhenDoingVerbosely() {
		RowDefinitionClassWrapper classWrapper = new RowDefinitionClassWrapper(MyExampleTableClass.class);
		RowDefinitionInstanceWrapper objectWrapper = classWrapper.instanceWrapperFor(obj);
		PropertyWrapper property = objectWrapper.getPropertyByColumn(database, "column1");
		if (property != null) {
			QueryableDatatype<?> qdt = property.getQueryableDatatype();
			property.setQueryableDatatype(qdt);
		}
	}

	@Test
	public void easyToGetInstanceWrapperGivenObject() {
		RowDefinitionInstanceWrapper objectWrapper = factory.instanceWrapperFor(obj);
	}

	@Test
	public void easyToIterateOverPropertiesUsingFactory() {
		RowDefinitionInstanceWrapper objectWrapper = factory.instanceWrapperFor(obj);
		for (PropertyWrapper property : objectWrapper.getColumnPropertyWrappers()) {
			QueryableDatatype<?> qdt = property.getQueryableDatatype();
			property.columnName();
			property.isForeignKey();
			property.isColumn();
			objectWrapper.isTable();
			objectWrapper.tableName();
		}
	}

	@DBTableName("table")
	public static class MyExampleTableClass extends DBRow {

		public static final long serialVersionUID = 1L;
		@DBPrimaryKey
		@DBColumn("column1")
		public DBInteger uid = new DBInteger();
	}
}
