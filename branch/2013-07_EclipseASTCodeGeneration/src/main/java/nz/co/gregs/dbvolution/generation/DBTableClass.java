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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author gregory.graham
 */
public class DBTableClass {

    String packageName;
    String className;
    String tableName;
    public String javaSource;
    List<DBTableField> fields = new ArrayList<DBTableField>();
    String lineSeparator = System.getProperty("line.separator");
    String conceptBreak = lineSeparator + lineSeparator;
    
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
            javaSrc.append("    @DBColumn(\"").append(field.columnName).append("\")");
            javaSrc.append(lineSeparator);
            if (field.isPrimaryKey) {
                javaSrc.append("    @DBPrimaryKey").append(lineSeparator);
            }
            if (field.isForeignKey) {
                javaSrc.append("    @DBForeignKey(").append(field.referencesClass).append(".class)");
                javaSrc.append(lineSeparator);
            }
            javaSrc.append("    public ").append(field.columnType).append(" ").append(field.fieldName).append(" = new ").append(field.columnType).append("();");
            javaSrc.append(conceptBreak);
        }
        javaSrc.append("}");
        javaSrc.append(conceptBreak);
        System.out.println(javaSrc.toString());

        this.javaSource = javaSrc.toString();
        return this.javaSource;
    }
}
