/*
 * Copyright 2013 gregory.graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.generation;

import java.io.File;
import java.util.Comparator;

import nz.co.gregs.dbvolution.generation.ast.ParsedBeanProperty;
import nz.co.gregs.dbvolution.generation.ast.ParsedClass;

/**
 * Maintains new and existing classes according to database schema information.
 */
public class ClassMaintainer {
	//private DBDefinition dbDefinition;
	private CodeGenerationConfiguration config;
	private TableNameResolver tableNameResolver;
	private ParsedClass parsedClass;
	
	/**
	 * Prepares for creating a brand new java file.
	 */
	public ClassMaintainer(CodeGenerationConfiguration config, TableNameResolver tableNameResolver) {
		//this.dbDefinition = dbDefinition;
		this.config = config;
		this.tableNameResolver = tableNameResolver;
		this.parsedClass = null;
	}
	
	/**
	 * Prepare for updating an existing java file.
	 */
	public ClassMaintainer(CodeGenerationConfiguration config, TableNameResolver tableNameResolver, ParsedClass parsedClass) {
		//this.dbDefinition = dbDefinition;
		this.config = config;
		this.tableNameResolver = tableNameResolver;
		this.parsedClass = parsedClass;
		if (parsedClass != null) {
			parsedClass.getTypeContext().setConfig(config);
		}
	}
	
	/**
	 * Update/generate java source to match database meta-data.
	 * @param dbTableClass meta-data to sync to
	 */
	public void ensureAs(DBTableClass dbTableClass) {
		ensureClassFor(dbTableClass);
		ensurePropertiesFor(dbTableClass);
		
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
			parsedClass = ParsedClass.newDBTableInstance(config, className, dbTableClass.getTableName());
		}
	}
	
	/**
	 * For each column that is:
	 * <ul>
	 * <li> missing on the target source file: add the standard property structure (field, getters/setters, etc.)
	 * <li> extra in the target source file: delete the associated field and getter/setters, regardless of modifications to them.
	 * </ul>
	 * where:
	 * <ul>
	 * <li> a property is identified by having a field or getter or setter with the appropriate annotation, and
	 * <li> fields and getter/setters are associated by property name as per standard bean specification
	 *      (ie: doesn't have to parse the methods).
	 * </ul>
	 * @param dbTableClass
	 */
	protected void ensurePropertiesFor(DBTableClass dbTableClass) {
		SetMatcher<ParsedBeanProperty, DBTableField> matches = new SetMatcher<ParsedBeanProperty, DBTableField>(
				parsedClass.getDBColumnProperties(), dbTableClass.fields,
				new Comparator<Object>(){
					@Override
					public int compare(Object o1, Object o2) {
						ParsedBeanProperty parsedProperty = (ParsedBeanProperty) o1;
						DBTableField tableField = (DBTableField) o2;
						
						// TODO: need to use equals/equalsIgnoreCase depending on database definition.
						String parsedColumnName = parsedProperty.getColumnNameIfSet();
						boolean equals = (parsedColumnName != null) && parsedColumnName.equalsIgnoreCase(tableField.getColumnName());
						return equals ? 0 : 1;
					}
				});
		
		ColumnNameResolver columnNameResolver = new ColumnNameResolver();
		
		// add missing properties
		// TODO (Config) do this if configured to do so (default = true)
		// TODO need to insert new fields/methods in position within existing that matches order of columns
		// TODO need to include handling of field/method name collisions
		for (DBTableField tableField: matches.getOnlyInB()) {
			ParsedBeanProperty newProperty = ParsedBeanProperty.newDBColumnInstance(parsedClass.getTypeContext(),
					columnNameResolver.getPropertyNameFor(tableField.getColumnName()),
					tableField.getColumnType(), tableField.isPrimaryKey(), tableField.getColumnName());
			if (newProperty.field() != null) {
				parsedClass.addFieldAfter(null, newProperty.field());
			}
			
			if (newProperty.getter() != null) {
				parsedClass.addMethodAfter(null, newProperty.getter());
			}
			
			if (newProperty.setter() != null) {
				parsedClass.addMethodAfter(null, newProperty.setter());
			}
		}
		
		// TODO - remove extra properties
		// TODO (Config) do this if configured to do so (default = true)
		
	}
}
