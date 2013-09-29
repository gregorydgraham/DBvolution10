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

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Represents the requirement for a code generated property that maps to a column.
 * The generated code will be a member field or getter/setter property, depending
 * on configuration.
 */
public class DBTableField {
    private String fieldName;
    private String columnName;
    private Class<? extends QueryableDatatype> columnType;
    private int precision;
    private boolean isPrimaryKey = false;
    private boolean isForeignKey = false;
    private String referencedTable; // set from schema and from existing source file foreign key mapping
    private String referencedColumn; // set from schema and from existing source file
    private DBTableClass referencedClass; // set during foreign key mapping
    private DBTableField referencedField; // set during foreign key mapping
    private String referencedClassName; // set from parsing existing source files
    
	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setIsPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	public boolean isForeignKey() {
		return isForeignKey;
	}

	public void setIsForeignKey(boolean isForeignKey) {
		this.isForeignKey = isForeignKey;
	}

	/**
	 * @return the referencedTable
	 */
	public String getReferencedTable() {
		return referencedTable;
	}

	/**
	 * @param referencedTable the referencedTable to set
	 */
	public void setReferencedTable(String referencedTable) {
		this.referencedTable = referencedTable;
	}

	/**
	 * @return the referencedColumn
	 */
	public String getReferencedColumn() {
		return referencedColumn;
	}

	/**
	 * @param referencedColumn the referencedColumn to set
	 */
	public void setReferencedColumn(String referencedColumn) {
		this.referencedColumn = referencedColumn;
	}

	public DBTableClass getReferencedClass() {
		return referencedClass;
	}

	public void setReferencedClass(DBTableClass referencesClass) {
		this.referencedClass = referencesClass;
	}

	public DBTableField getReferencedField() {
		return referencedField;
	}

	public void setReferencedField(DBTableField referencesField) {
		this.referencedField = referencesField;
	}

	public Class<? extends QueryableDatatype> getColumnType() {
		return columnType;
	}

	public void setColumnType(Class<? extends QueryableDatatype> columnType) {
		this.columnType = columnType;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	/**
	 * @return the referencedClassName
	 */
	public String getReferencedClassName() {
		return referencedClassName;
	}

	/**
	 * @param referencedClassName the referencedClassName to set
	 */
	public void setReferencedClassName(String referencedClassName) {
		this.referencedClassName = referencedClassName;
	}
   
}
