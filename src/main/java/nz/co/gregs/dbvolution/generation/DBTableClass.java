/*
 * Copyright 2013 Gregory Graham.
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

import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBUnknownDatatype;

/**
 *
 * @author Gregory Graham
 */
public class DBTableClass {

	private static final long serialVersionUID = 1L;

	private final Class<DBUnknownDatatype> unknownDatatype = DBUnknownDatatype.class;
	private final String packageName;
	private final String className;
	private final String tableName;
	private final String tableSchema;
	private final List<DBTableField> fields = new ArrayList<DBTableField>();
	private String javaSource;
	private Class<? extends DBRow> generatedClass;
	private DBRow generatedInstance;

	/**
	 * Constructor with required information for automatically creating a DBRow
	 * class.
	 *
	 * @param tableName tableName
	 * @param tableSchema the schema the table is contained within.
	 * @param packageName packageName
	 * @param className className
	 */
	public DBTableClass(String tableName, String tableSchema, String packageName, String className) {
		this.tableName = tableName;
		this.tableSchema = tableSchema;
		this.packageName = packageName;
		this.className = className;
	}

	/**
	 * Returns the package and class name formatted for use in Java code.
	 *
	 * <p>
	 * For a class named AClass in the package com.acme.database {@link #getFullyQualifiedName()
	 * } will return "com.acme.database.AClass".
	 *
	 * @return a String of the fully qualified class name.
	 */
	public String getFullyQualifiedName() {
		return this.getPackageName() + "." + getClassName();
	}

	/**
	 *
	 * @return the packageName
	 */
	public String getPackageName() {
		return packageName;
	}

	/**
	 *
	 * @return the fields
	 */
	public List<DBTableField> getFields() {
		return fields;
	}

	/**
	 *
	 * @return the javaSource
	 */
	public String getJavaSource() {
		return javaSource;
	}

	/**
	 *
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 *
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * @return the tableSchema
	 */
	public String getTableSchema() {
		return tableSchema;
	}

	/**
	 * @return the unknownDatatype
	 */
	public Class<DBUnknownDatatype> getUnknownDatatype() {
		return unknownDatatype;
	}

	void setGeneratedInstance(DBRow generatedClassObject) {
		this.generatedInstance = generatedClassObject;
	}

	DBRow getGeneratedInstance() {
		return this.generatedInstance;
	}

	void setGeneratedClass(Class<? extends DBRow> generatedClass) {
		this.generatedClass = generatedClass;
	}

	Class<? extends DBRow> getGeneratedClass() {
		return this.generatedClass;
	}

	/**
	 * @param javaSource the javaSource to set
	 */
	void setJavaSource(String javaSource) {
		this.javaSource = javaSource;
	}
}
