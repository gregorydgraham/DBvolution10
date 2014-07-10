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
 *
 * @author Gregory Graham
 */
public class DBTableField {
    public String fieldName;
    public String columnName;
    public boolean isPrimaryKey = false;
    public boolean isForeignKey = false;
    public String referencesClass;
    public String referencesField;
    public Class<? extends Object> columnType;
    public int precision;
    public int javaSQLDatatype = 0;
	public String comments;
	public boolean isAutoIncrement;
	public int sqlDataTypeInt;
    
}
