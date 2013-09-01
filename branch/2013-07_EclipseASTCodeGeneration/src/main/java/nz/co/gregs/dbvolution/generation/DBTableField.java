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
 *
 * @author gregory.graham
 */
public class DBTableField {
    String fieldName;
    String columnName;
    boolean isPrimaryKey = false;
    boolean isForeignKey = false;
    String referencesClass;
    String referencesField;
    Class<? extends QueryableDatatype> columnType;
    int precision;
    
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

	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	public boolean isForeignKey() {
		return isForeignKey;
	}

	public void setForeignKey(boolean isForeignKey) {
		this.isForeignKey = isForeignKey;
	}

	public String getReferencesClass() {
		return referencesClass;
	}

	public void setReferencesClass(String referencesClass) {
		this.referencesClass = referencesClass;
	}

	public String getReferencesField() {
		return referencesField;
	}

	public void setReferencesField(String referencesField) {
		this.referencesField = referencesField;
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
    
}
