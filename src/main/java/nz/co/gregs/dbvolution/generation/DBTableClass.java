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
 * @author Gregory Graham
 */
public class DBTableClass {

    public long serialversionUIDBValue = 1L;

    private final String unknownDatatype = DBUnknownDatatype.class.getSimpleName();
    private String packageName;
    public String className;
    private String tableName;
    private String javaSource;
    private final List<DBTableField> fields = new ArrayList<DBTableField>();
    private final String lineSeparator = System.getProperty("line.separator");
    private final String conceptBreak = lineSeparator + lineSeparator;

    public String getFullyQualifiedName() {
        return this.getPackageName() + "." + className;
    }

    public String generateJavaSource() {
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
        javaSrc.append("import ").append(importPackageName).append(".annotations.*;");
        javaSrc.append(conceptBreak);

        final String tableNameAnnotation = DBTableName.class.getSimpleName();
        final String dbRowClassName = DBRow.class.getSimpleName();
        final String dbColumnAnnotation = DBColumn.class.getSimpleName();
        final String primaryKeyAnnotation = DBPrimaryKey.class.getSimpleName();
        final String foreignKeyAnnotation = DBForeignKey.class.getSimpleName();
        final String unknownJavaSQLTypeAnnotation = DBUnknownJavaSQLType.class.getSimpleName();

        javaSrc.append("@").append(tableNameAnnotation).append("(\"").append(this.getTableName()).append("\") ");
        javaSrc.append(lineSeparator);
        javaSrc.append("public class ").append(this.className).append(" extends ").append(dbRowClassName).append(" {");
        javaSrc.append(conceptBreak);

        javaSrc.append("    public static final long serialVersionUID = ").append(serialversionUIDBValue).append("L;");
        javaSrc.append(conceptBreak);

        for (DBTableField field : getFields()) {
            javaSrc.append("    @").append(dbColumnAnnotation).append("(\"").append(field.columnName).append("\")");
            javaSrc.append(lineSeparator);
            if (field.isPrimaryKey) {
                javaSrc.append("    @").append(primaryKeyAnnotation).append(lineSeparator);
            }
            if (field.isForeignKey) {
                javaSrc.append("    @").append(foreignKeyAnnotation).append("(").append(field.referencesClass).append(".class)");
                javaSrc.append(lineSeparator);
            }
            if (unknownDatatype.equals(field.columnType)) {

                javaSrc.append("    @").append(unknownJavaSQLTypeAnnotation).append("(").append(field.javaSQLDatatype).append(")");
                javaSrc.append(lineSeparator);
            }
            javaSrc.append("    public ").append(field.columnType).append(" ").append(field.fieldName).append(" = new ").append(field.columnType).append("();");
            javaSrc.append(conceptBreak);
        }
        javaSrc.append("}");
        javaSrc.append(conceptBreak);

        this.javaSource = javaSrc.toString();
        return this.getJavaSource();
    }

    /**
     * @return the packageName
     */
    public String getPackageName() {
        return packageName;
    }

    /**
     * @param packageName the packageName to set
     */
    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    /**
     * @return the fields
     */
    public List<DBTableField> getFields() {
        return fields;
    }

    /**
     * @return the javaSource
     */
    public String getJavaSource() {
        return javaSource;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
