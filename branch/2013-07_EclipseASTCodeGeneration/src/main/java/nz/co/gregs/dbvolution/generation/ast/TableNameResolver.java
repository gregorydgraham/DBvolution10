package nz.co.gregs.dbvolution.generation.ast;

import java.util.HashMap;
import java.util.Map;

import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;


/**
 * Helps to resolve database table names to class names,
 * and vice-versa, while avoiding duplications of names.
 * 
 * <p> Requires you to create an instance for each package
 * that you wish to manage.
 * @author Malcolm Lett
 */
// TODO: consider moving into nz.co.gregs.dbvolution.generation package
// TODO: implement smarts
public class TableNameResolver {
	// maps table-name to fully-qualified-class-name
	// maps to null to represent that table/class name is known of, but has no mapping as yet
	private Map<String,String> tableNameToClassNameMap = new HashMap<String, String>();
	private Map<String,String> classNameToTableNameMap = new HashMap<String, String>();
	private Map<String,ParsedClass> classNameToClassMap = new HashMap<String, ParsedClass>();
	private String packageName; // qualified name
	
	public TableNameResolver(String packageName) {
		this.packageName = packageName;
	}
	
	public void addTable(String tableName) {
		// don't overwrite if already have map from databaseName to class
		if (!tableNameToClassNameMap.containsKey(tableName)) {
			tableNameToClassNameMap.put(tableName, null);
		}
	}

	public void addClass(String className, String tableName) {
		classNameToTableNameMap.put(className, tableName);
	}
	
	public void addClass(ParsedClass parsedClass) {
		String tableName = parsedClass.getDBTableNameIfSet(); // may be null
		classNameToTableNameMap.put(parsedClass.getFullyQualifiedName(), tableName);
		classNameToClassMap.put(parsedClass.getFullyQualifiedName(), parsedClass);
	}
	
	/**
	 * Gets the unique class name allocated to the table.
	 * @param tableName
	 * @return the simple class name
	 */
	// TODO: actually implement duplication avoidance logic
	public String getClassNameFor(String tableName) {
		String className = tableNameToClassNameMap.get(tableName);
		if (className != null) {
			return className;
		}
		
		String simpleClassName = DBTableClassGenerator.toClassCase(tableName);
		className = ((packageName == null) ? "" : packageName+".") + simpleClassName;
		addClass(className, tableName);
		return simpleClassName;
	}
	
	/**
	 * Gets the fully qualified unique class name allocated to the table.
	 * @param tableName
	 * @return the fully qualified class name
	 */
	public String getQualifiedClassNameFor(String tableName) {
		String simpleClassName = getClassNameFor(tableName);
		String className = ((packageName == null) ? "" : packageName+".") + simpleClassName;
		return className;
	}
	
	
}
