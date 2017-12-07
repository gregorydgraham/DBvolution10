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

/**
 * Stores information needed to automatically create a Java field from a
 * database column.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class DBTableField {

	/**
	 * Stores the Java name of the DBTableField.
	 *
	 */
	public String fieldName;

	/**
	 * Stores the actual database name of the DBTableField.
	 */
	public String columnName;

	/**
	 * TRUE if the column/field is a Primary Key DBTableField, otherwise FALSE.
	 *
	 */
	public boolean isPrimaryKey = false;

	/**
	 * TRUE if the column/field is a Foreign Key DBTableField, otherwise FALSE.
	 *
	 */
	public boolean isForeignKey = false;

	/**
	 * Stores the name of the class referenced by this DBTableField if it is a
	 * Foreign Key, otherwise null.
	 */
	public String referencesClass;

	/**
	 * Stores the name of the field in the referenced class referenced by this
	 * column/field if this DBTableField is a foreign key, otherwise null.
	 *
	 */
	public String referencesField;

	/**
	 * Stores the class of the Java field that will be created.
	 *
	 */
	public Class<? extends Object> columnType;

	/**
	 * Stores the precision of the column.
	 *
	 */
	public int precision;

	/**
	 * Stores the datatype reported by Java in case an unknown datatype is
	 * reported.
	 *
	 */
	public int javaSQLDatatype = 0;

	/**
	 * Stores column comments/remarks from the database.
	 *
	 */
	public String comments;

	/**
	 * TRUE if the DBTableField is an auto-incrementing column, otherwise FALSE.
	 *
	 */
	public boolean isAutoIncrement;

	/**
	 * Stores the data type of the DBTableField as reported from the database.
	 */
	public int sqlDataTypeInt;

	/**
	 * Stores the data type name of the DBTableField as reported from the
	 * database.
	 */
	public String sqlDataTypeName;
	String referencedTable;

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DBTableField) {
			DBTableField other = (DBTableField) obj;
			return fieldName.equals(other.fieldName);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 79 * hash + (this.fieldName != null ? this.fieldName.hashCode() : 0);
		hash = 79 * hash + (this.columnName != null ? this.columnName.hashCode() : 0);
		hash = 79 * hash + (this.isPrimaryKey ? 1 : 0);
		hash = 79 * hash + (this.isForeignKey ? 1 : 0);
		hash = 79 * hash + (this.referencesClass != null ? this.referencesClass.hashCode() : 0);
		hash = 79 * hash + (this.referencesField != null ? this.referencesField.hashCode() : 0);
		hash = 79 * hash + (this.columnType != null ? this.columnType.hashCode() : 0);
		hash = 79 * hash + this.precision;
		hash = 79 * hash + this.javaSQLDatatype;
		hash = 79 * hash + (this.comments != null ? this.comments.hashCode() : 0);
		hash = 79 * hash + (this.isAutoIncrement ? 1 : 0);
		hash = 79 * hash + this.sqlDataTypeInt;
		return hash;
	}

}
