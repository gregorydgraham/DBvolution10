package nz.co.gregs.dbvolution.ast;

import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.generation.DBTableClass;
import nz.co.gregs.dbvolution.generation.DBTableClassCodeGeneratorAndUpdater;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.generation.ast.TableNameResolver;

import org.junit.Test;

public class DBTableClassCodeGeneratorAndUpdaterTest extends AbstractASTTest {
	@Test
	public void generateFromMetaData() {
		TableNameResolver resolver = new TableNameResolver(DBTableClassCodeGeneratorAndUpdaterTest.class.getPackage().getName());
		DBTableClassCodeGeneratorAndUpdater updater = new DBTableClassCodeGeneratorAndUpdater(resolver);
		
		DBTableClass tableClass = new DBTableClass();
		tableClass.setTableName(DBTableClassCodeGeneratorAndUpdaterTest.class.getSimpleName()+"_1");
		
		tableClass.getFields().add(field("uid", DBNumber.class, true));
		tableClass.getFields().add(field("name", DBString.class, false));
		tableClass.getFields().add(field("age", DBNumber.class, false));
		
		
		updater.ensureAs(tableClass);
	}
	
	private static DBTableField field(String columnName, Class<? extends QueryableDatatype> columnType, boolean isPrimaryKey) {
		DBTableField field = new DBTableField();
		field.setColumnName(columnName);
		field.setColumnType(columnType);
		field.setPrimaryKey(isPrimaryKey);
		return field;
	}
}
