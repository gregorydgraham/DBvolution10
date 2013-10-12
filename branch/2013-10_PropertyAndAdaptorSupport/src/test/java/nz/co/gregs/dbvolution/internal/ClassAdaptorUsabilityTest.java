package nz.co.gregs.dbvolution.internal;

import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

import org.junit.Test;

public class ClassAdaptorUsabilityTest {
	private DBDefinition dbDefn = new DBDefinition();
	private MyExampleTableClass obj = new MyExampleTableClass();
	
	@Test
	public void easyToGetSpecificPropertyValueOnObjectWhenDoingInline() {
		QueryableDatatype qdt = new ClassAdaptor(MyExampleTableClass.class)
			.objectAdaptorFor(dbDefn, obj).getPropertyByColumn("column1").value();
	}

	@Test
	public void easyToGetSpecificPropertyValueOnObjectWhenDoingVerbosely() {
		ClassAdaptor classAdaptor = new ClassAdaptor(MyExampleTableClass.class);
		ObjectAdaptor objectAdaptor = classAdaptor.objectAdaptorFor(dbDefn, new MyExampleTableClass());
		DBProperty property = objectAdaptor.getPropertyByColumn("column1");
		if (property != null) {
			QueryableDatatype qdt = property.value();
		}
	}
	
	@Test
	// requires: private ClassAdaptorCache adaptedClasses = new ClassAdaptorCache();
	public void easyToGetObjectAdaptorGivenObject() {
		//ObjectAdaptor objectAdaptor = adaptedClasses.objectAdaptorFor(dbDefn, obj);
		throw new UnsupportedOperationException("not yet implemented");
	}
	
	@DBTableName("table")
	public static class MyExampleTableClass {
		@DBPrimaryKey
		@DBColumn("column1")
		public DBInteger uid = new DBInteger();
	}

}
