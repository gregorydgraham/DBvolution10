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
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the requirement for a code generated class that maps
 * to a database table.
 */
public class DBTableClass {
    private File javaFile; // existing java source file
    private String tableName;
    private String packageName; // TODO: remove
    private String className; // resolved simpleName, TODO: make fully qualified
    private String javaSource; // TODO: remove
    private List<DBTableField> fields = new ArrayList<DBTableField>();
    private static final String lineSeparator = System.getProperty("line.separator"); // TODO remove
    private static final String conceptBreak = lineSeparator + lineSeparator; // TODO remove
    
    public String getPackageName() {
		return packageName;
	}

    public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<DBTableField> getFields() {
		return fields;
	}

	public void setFields(List<DBTableField> fields) {
		this.fields = fields;
	}
	
	/**
	 * Gets the field that maps to the given column name.
	 * Note: if multiple fields map to the same column, this
	 * method returns the first found.
	 * @param columnName
	 * @return the field or null if not found
	 */
	public DBTableField getFieldByColumnName(String columnName) {
		if (fields != null) {
			for (DBTableField field: fields) {
				// TODO: need to use ignoreCase/caseSensitive depending on database definition
				if (field.getColumnName().equalsIgnoreCase(columnName)) {
					return field;
				}
			}
		}
		return null; // not found
	}

	/**
	 * @return the javaFile
	 */
	public File getJavaFile() {
		return javaFile;
	}

	/**
	 * @param javaFile the javaFile to set
	 */
	public void setJavaFile(File javaFile) {
		this.javaFile = javaFile;
	}
	
	public String getJavaSource() {
		return javaSource;
	}

	public void setJavaSource(String javaSource) {
		this.javaSource = javaSource;
	}
	
	public String generateJavaSource() {
        StringBuilder javaSrc = new StringBuilder();
        if (this.packageName != null) {
            javaSrc.append("package ").append(this.packageName).append(";");
            javaSrc.append(conceptBreak);
        }
        javaSrc.append("import nz.co.gregs.dbvolution.*;");
        javaSrc.append(lineSeparator);
        javaSrc.append("import nz.co.gregs.dbvolution.annotations.*;");
        javaSrc.append(conceptBreak);

        javaSrc.append("@DBTableName(\"").append(this.tableName).append("\") ");
        javaSrc.append(lineSeparator);
        javaSrc.append("public class ").append(this.className).append(" extends DBRow {");
        javaSrc.append(conceptBreak);

        for (DBTableField field : fields) {
            javaSrc.append("    @DBColumn(\"").append(field.getColumnName()).append("\")");
            javaSrc.append(lineSeparator);
            if (field.isPrimaryKey()) {
                javaSrc.append("    @DBPrimaryKey").append(lineSeparator);
            }
            if (field.isForeignKey()) {
            	String className = (field.getReferencedClass() == null) ? "null" : field.getReferencedClass().getClassName();
                javaSrc.append("    @DBForeignKey(").append(className).append(".class)");
                javaSrc.append(lineSeparator);
            }
            javaSrc.append("    public ").append(field.getColumnType()).append(" ").append(field.getFieldName()).append(" = new ").append(field.getColumnType()).append("();");
            javaSrc.append(conceptBreak);
        }
        javaSrc.append("}");
        javaSrc.append(conceptBreak);
        System.out.println(javaSrc.toString());

        this.javaSource = javaSrc.toString();
        return this.javaSource;
        //return javaSrc.toString();
    }
}
