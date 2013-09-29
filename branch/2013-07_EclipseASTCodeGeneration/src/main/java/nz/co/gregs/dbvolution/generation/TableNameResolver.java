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

import java.util.HashMap;
import java.util.Map;

import nz.co.gregs.dbvolution.generation.ast.ParsedClass;


/**
 * Helps to resolve database table names to class names,
 * and vice-versa, while avoiding duplications of names.
 *
 * <p> The collection handling mechanism is:
 * <ul>
 * <li> initially try the default formatted class name, then
 * <li> if there is a collision, try incrementing values from 1 upwards
 * and format the classname as: <code>"packagePath.className&lt;count&gt"<code>.
 * </ul>
 * 
 * <p> Requires you to create an instance for each package
 * that you wish to manage.
 */
// TODO: change packageName to a modifiable path that is added to each newly
//       resolved class names with the value at the time the class name is resolved.
//       And use qualified class names everywhere. Then only one instance is needed
//       across all packages.
public class TableNameResolver {
	// maps table-name to fully-qualified-class-name
	// maps to null to represent that table/class name is known of, but has no mapping as yet
	// (class names are stored in their simple name form)
	private Map<String,String> tableNameToClassNameMap = new HashMap<String, String>();
	private Map<String,String> classNameToTableNameMap = new HashMap<String, String>();
	private Map<String,ParsedClass> classNameToClassMap = new HashMap<String, ParsedClass>();
	private String packageName; // qualified name
	
	public TableNameResolver(String packageName) {
		this.packageName = packageName;
	}
	
	/**
	 * Adds 
	 * @param tableName
	 */
	public void addTable(String tableName) {
		// don't overwrite if already have map from databaseName to class
		if (!tableNameToClassNameMap.containsKey(tableName)) {
			tableNameToClassNameMap.put(tableName, null);
		}
	}

	/**
	 * 
	 * @param className simple name of class only
	 * @param tableName
	 */
	public void addClass(String className, String tableName) {
		classNameToTableNameMap.put(className, tableName);
	}
	
	public void addClass(ParsedClass parsedClass) {
		String tableName = parsedClass.getTableNameIfSet(); // may be null
		classNameToTableNameMap.put(parsedClass.getFullyQualifiedName(), tableName);
		classNameToClassMap.put(parsedClass.getFullyQualifiedName(), parsedClass);
	}
	
	/**
	 * Gets the unique class name allocated to the table.
	 * @param tableName
	 * @return the simple class name
	 */
	public String getSimpleClassNameFor(String tableName) {
		String className = getQualifiedClassNameFor(tableName);
		int dotIndex = className.lastIndexOf('.');
		if (dotIndex >= 0) {
			return className.substring(dotIndex+1);
		}
		return className;
	}
	
	/**
	 * Gets the fully qualified unique class name allocated to the table.
	 * @param tableName
	 * @return the fully qualified class name
	 */
	// TODO: actually implement duplication avoidance logic
	public String getQualifiedClassNameFor(String tableName) {
		String className = tableNameToClassNameMap.get(tableName);
		if (className != null) {
			return className;
		}
		
		String simpleClassName = DBTableClassGenerator.toClassCase(tableName);
		className = (packageName == null) ? simpleClassName : packageName+"."+simpleClassName;
		
		// apply collision avoidance
		int postfix = 1;
		String proposedClassName = className;
		while(classNameToTableNameMap.containsKey(proposedClassName)) {
			proposedClassName = className+postfix;
			postfix++;
		}
		
		addClass(className, tableName);
		return className;
	}
	
	
}
