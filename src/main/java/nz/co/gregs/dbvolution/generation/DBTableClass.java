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
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.datatypes.DBUnknownDatatype;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBTableClass {

	private static final long serialVersionUID = 1L;

	private final Class<DBUnknownDatatype> unknownDatatype = DBUnknownDatatype.class;
	private String packageName;
	private String className;
	private String tableName;
	private final String tableSchema;
	private String javaSource;
	private final List<DBTableField> fields = new ArrayList<DBTableField>();
	private final String lineSeparator = System.getProperty("line.separator");
	private final String conceptBreak = lineSeparator + lineSeparator;

	/**
	 * Constructor with required information for automatically creating a DBRow
	 * class.
	 *
	 * @param tableName tableName
	 * @param tableSchema
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String of the fully qualified class name.
	 */
	public String getFullyQualifiedName() {
		return this.getPackageName() + "." + getClassName();
	}

	/**
	 * Transforms the information encapsulated within the DBTableClass into valid
	 * Java source code.
	 *
	 * <p>
	 * After all available information has been set for this DBTableClass, this
	 * method is called to generate the required Java source.
	 *
	 * @param options
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String of the source code of the new DBRow class.
	 */
	public String generateJavaSource(DBTableClassGenerator.Options options) {
		StringBuilder javaSrc = new StringBuilder();
		final String outputPackageName = this.getPackageName();
		if (outputPackageName != null) {
			javaSrc.append("package ").append(outputPackageName).append(";");
			javaSrc.append(conceptBreak);
		}

		final String importPackageName = DBRow.class.getPackage().getName();
		javaSrc.append("import ").append(importPackageName).append(".*;");
		javaSrc.append(lineSeparator);
		javaSrc.append("import ").append(importPackageName).append(".datatypes.*;");
		javaSrc.append(lineSeparator);
		javaSrc.append("import ").append(importPackageName).append(".datatypes.spatial2D.*;");
		javaSrc.append(lineSeparator);
		javaSrc.append("import ").append(importPackageName).append(".annotations.*;");
		javaSrc.append(conceptBreak);

		final String tableNameAnnotation = DBTableName.class.getSimpleName();
		final String dbRowClassName = DBRow.class.getSimpleName();
		final String dbColumnAnnotation = DBColumn.class.getSimpleName();
		final String primaryKeyAnnotation = DBPrimaryKey.class.getSimpleName();
		final String autoIncrementAnnotation = DBAutoIncrement.class.getSimpleName();
		final String foreignKeyAnnotation = DBForeignKey.class.getSimpleName();
		final String unknownJavaSQLTypeAnnotation = DBUnknownJavaSQLType.class.getSimpleName();

		if (this.tableSchema == null || "PUBLIC".equals(tableSchema.toUpperCase()) || "dbo".equals(tableSchema)) {
			javaSrc.append("@").append(tableNameAnnotation).append("(\"").append(this.getTableName()).append("\") ");
		} else {
			javaSrc.append("@").append(tableNameAnnotation).append("(value=\"").append(this.getTableName()).append("\", schema=\"").append(this.tableSchema).append("\") ");
		}

		javaSrc.append(lineSeparator);
		javaSrc.append("public class ").append(this.getClassName()).append(" extends ").append(dbRowClassName).append(" {");
		javaSrc.append(conceptBreak);

		javaSrc.append("    public static final long serialVersionUID = ").append(options.versionNumber).append("L;");
		javaSrc.append(conceptBreak);

		for (DBTableField field : getFields()) {
			if (field.comments == null || field.comments.isEmpty()) {
				javaSrc.append("    @").append(dbColumnAnnotation).append("(\"").append(field.columnName).append("\")");
			} else {
				javaSrc.append("    @").append(dbColumnAnnotation).append("(value=\"").append(field.columnName).append("\"")
						.append(", comments=\"").append(field.comments.replaceAll("\"", "\\\"")).append(")");
			}
			javaSrc.append(lineSeparator);
			if (field.isPrimaryKey) {
				javaSrc.append("    @").append(primaryKeyAnnotation).append(lineSeparator);
			}
			if (field.isAutoIncrement) {
				javaSrc.append("    @").append(autoIncrementAnnotation).append(lineSeparator);
			}
			if (field.isForeignKey) {
				if (options.includeForeignKeyColumnName) {
					javaSrc.append("    @").append(foreignKeyAnnotation).append("(value = ").append(field.referencesClass).append(".class, column = \"").append(field.referencesField).append("\")");
				} else {
					javaSrc.append("    @").append(foreignKeyAnnotation).append("(").append(field.referencesClass).append(".class)");
				}
				javaSrc.append(lineSeparator);
			}
			if (unknownDatatype.equals(field.columnType)) {

				javaSrc.append("    @").append(unknownJavaSQLTypeAnnotation).append("(").append(field.javaSQLDatatype).append(")");
				javaSrc.append(lineSeparator);
			}
			javaSrc.append("    public ").append(field.columnType.getSimpleName()).append(" ").append(field.fieldName).append(" = new ").append(field.columnType.getSimpleName()).append("();");
			javaSrc.append(conceptBreak);
		}
		javaSrc.append("}");
		javaSrc.append(conceptBreak);

		this.javaSource = javaSrc.toString();
		return this.getJavaSource();
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the packageName
	 */
	public String getPackageName() {
		return packageName;
	}

	/**
	 * @param packageName the packageName to set
	 */
	private void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the fields
	 */
	public List<DBTableField> getFields() {
		return fields;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the javaSource
	 */
	public String getJavaSource() {
		return javaSource;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName the tableName to set
	 */
	private void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * @param className the className to set
	 */
	private void setClassName(String className) {
		this.className = className;
	}
}
