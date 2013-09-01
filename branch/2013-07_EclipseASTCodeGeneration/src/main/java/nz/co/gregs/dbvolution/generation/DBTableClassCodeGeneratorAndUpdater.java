package nz.co.gregs.dbvolution.generation;

import java.io.File;
import java.util.Comparator;

import nz.co.gregs.dbvolution.ast.LowLevelGenerationTests;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.generation.ast.ColumnNameResolver;
import nz.co.gregs.dbvolution.generation.ast.ParsedClass;
import nz.co.gregs.dbvolution.generation.ast.ParsedField;
import nz.co.gregs.dbvolution.generation.ast.TableNameResolver;

/**
 * Used to generate brand new classes and to synchronise existing ones with schema
 * meta-data.
 */
// TODO: name this better, so far named weirldy to avoid conflict with existing classes
public class DBTableClassCodeGeneratorAndUpdater {
	//private DBDefinition dbDefinition;
	private TableNameResolver tableNameResolver;
	private ParsedClass parsedClass;
	
	/**
	 * Prepares for creating a brand new java file.
	 */
	public DBTableClassCodeGeneratorAndUpdater(TableNameResolver tableNameResolver) {
		//this.dbDefinition = dbDefinition;
		this.tableNameResolver = tableNameResolver;
		this.parsedClass = null;
	}
	
	/**
	 * Prepare for updating an existing java file.
	 */
	public DBTableClassCodeGeneratorAndUpdater(TableNameResolver tableNameResolver, ParsedClass parsedClass) {
		//this.dbDefinition = dbDefinition;
		this.tableNameResolver = tableNameResolver;
		this.parsedClass = parsedClass;
	}
	
	/**
	 * Update/generate java source to match database meta-data.
	 * @param dbTableClass meta-data to sync to
	 */
	public void ensureAs(DBTableClass dbTableClass) {
		ensureClassFor(dbTableClass);
		ensureFieldsFor(dbTableClass);
		
		File srcFolder = new File("target/test-output"); // TODO: this needs to be specified somewhere
		srcFolder.mkdirs();
		parsedClass.writeToSourceFolder(srcFolder);
	}
	
	protected void ensureClassFor(DBTableClass dbTableClass) {
		// validate table name matches if java file already exists
		if (parsedClass != null) {
			String tableName = parsedClass.getDBTableNameIfSet();
			if (tableName == null) {
				// TODO: need to check that the defaulting mechanism is in place for
				// inferring the table name from the class name so long as @DBTable is included
				throw new IllegalArgumentException("Class "+parsedClass.getFullyQualifiedName()+
						" does not have a DB Table name");
			}
			else if (!tableName.equalsIgnoreCase(dbTableClass.getTableName())) {
				// TODO: should only use case-insensitive matching if that behaviour's in the definition of the database
				throw new IllegalArgumentException("Class "+parsedClass.getFullyQualifiedName()+
						" cannot be synced with DB Table "+dbTableClass.getTableName()+
						": already mapped to table "+tableName);
			}
		}
		
		// generate original file if not already exists
		if (parsedClass == null) {
			String className = tableNameResolver.getQualifiedClassNameFor(dbTableClass.getTableName());
			parsedClass = ParsedClass.newDBTableInstance(className, dbTableClass.getTableName());
		}
	}
	
	protected void ensureFieldsFor(DBTableClass dbTableClass) {
		SetMatcher<ParsedField, DBTableField> matches = new SetMatcher<ParsedField, DBTableField>(
				parsedClass.getFields(), dbTableClass.fields,
				new Comparator<Object>(){
					@Override
					public int compare(Object o1, Object o2) {
						ParsedField parsedField = (ParsedField) o1;
						DBTableField tableField = (DBTableField) o2;
						
						// TODO: need to use equals/equalsIgnoreCase depending on database definition.
						String parsedColumnName = parsedField.getColumnNameIfSet();
						boolean equals = (parsedColumnName != null) && parsedColumnName.equalsIgnoreCase(tableField.getColumnName());
						return equals ? 0 : 1;
					}
				});
		
		ColumnNameResolver columnNameResolver = new ColumnNameResolver();
		
		// add missing fields
		for (DBTableField tableField: matches.getOnlyInB()) {
			ParsedField newField = ParsedField.newDBTableColumnInstance(parsedClass.getTypeContext(),
					columnNameResolver.getPropertyNameFor(tableField.getColumnName()),
					tableField.getColumnType(), tableField.isPrimaryKey(), tableField.getColumnName());
			parsedClass.addFieldAfter(null, newField);
			
			// TODO: generate getter/setter methods too
		}
		
	}
}
