package nz.co.gregs.dbvolution.internal;

import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;

/**
 * Wraps a specific target object according to its type's {@link DBRowClassWrapper}.
 * To create instances of this type, call {@link DBRowClassWrapper#instanceAdaptorFor(DBDefinition, Object)}
 * on the appropriate {@link DBRowClassWrapper}. 
 * 
 * <p> Instances of this class are lightweight and efficient to create,
 * and they are intended to be short lived.
 * Instances of this class must not be shared between different DBDatabase instances,
 * however they can be safely associated within a single DBDatabase instance.
 * 
 * <p> Instances of this class are <i>thread-safe</i>.
 * @author Malcolm Lett
 */
public class DBRowInstanceWrapper {
	private final DBDatabase database;
	private final DBRowClassWrapper classWrapper;
	private final Object target;
	
	/**
	 * Called by {@link DBRowClassWrapper#instanceAdaptorFor(DBDefinition, Object)}.
	 * @param dbDefn
	 * @param classWrapper
	 * @param target the target object of the same type as analysed by {@code classAdaptor}
	 */
	DBRowInstanceWrapper(DBDatabase database, DBRowClassWrapper classWrapper, Object target) {
		if (target == null) {
			throw new DBRuntimeException("Target object is null");
		}
		if (!classWrapper.adaptee().isInstance(target)) {
			throw new DBRuntimeException("Target object's type ("+target.getClass().getName()+
					") is not compatible with given class adaptor for type "+classWrapper.adaptee().getName()+
					" (this is probably a bug in DBvolution)");
		}
		
		this.database = database;
		this.target = target;
		this.classWrapper = classWrapper;
	}

	/**
	 * Gets a string representation suitable for debugging.
	 */
	@Override
	public String toString() {
		if (isTable()) {
			return getClass().getSimpleName()+"<"+tableName()+":"+classWrapper.adaptee().getName()+">";
		}
		else {
			return getClass().getSimpleName()+"<no-table:"+classWrapper.adaptee().getName()+">";
		}
	}
	
	/**
	 * Gets the wrapped object type supported by this {@code ObjectAdaptor}.
	 * Note: this should be the same as the wrapped object's actual type.
	 * @return
	 */
	public Class<?> adapteeType() {
		return classWrapper.adaptee();
	}
	
	/**
	 * Gets the object wrapped by this {@code ObjectAdaptor}.
	 * @return
	 */
	public Object adapteeObject() {
		return target;
	}
	
	/**
	 * Gets the simple name of the class being wrapped by this adaptor.
	 * <p> Use {@link #tableName()} for the name of the table mapped to this class.
	 * @return
	 */
	public String javaName() {
		return classWrapper.javaName();
	}
	
	/**
	 * Gets the fully qualified name of the class being wrapped by this adaptor.
	 * <p> Use {@link #tableName()} for the name of the table mapped to this class.
	 * @return
	 */
	public String qualifiedJavaName() {
		return classWrapper.qualifiedJavaName();
	}
	
	/**
	 * Indicates whether this class maps to a database column.
	 * @return
	 */
	public boolean isTable() {
		return classWrapper.isTable();
	}
	
	/**
	 * Gets the indicated table name.
	 * Applies defaulting if the {@link DBTableName} annotation is present but doesn't provide
	 * an explicit table name.
	 * 
	 * <p> If the {@link DBTableName} annotation is missing, this method returns {@code null}.
	 * 
	 * <p> Use {@link #getDBTableNameAnnotation} for low level access.
	 * @return the table name, if specified explicitly or implicitly.
	 */
	public String tableName() {
		return classWrapper.tableName();
	}
	
	/**
	 * Gets the properties that together form the primary key, if any are marked.
	 * In most tables this will be exactly one property.
	 * @return the non-empty list of properties, or null if no primary key
	 */
	public List<DBPropertyDefinition> primaryKey() {
		return classWrapper.primaryKey();
	}
	
	/**
	 * Gets the property associated with the given column.
	 * If multiple properties are annotated for the same column, this method
	 * will return only the first.
	 * 
	 * <p> Only provides access to properties annotated with {@code DBColumn}.
	 * 
	 * <p> Assumes validation is applied elsewhere to prohibit duplication of 
	 * column names.
	 * @param dbDefn active database definition
	 * @param columnName
	 * @return
	 */
	public DBProperty getDBPropertyByColumn(String columnName) {
		DBPropertyDefinition classProperty = classWrapper.getPropertyByColumn(database, columnName);
		return (classProperty == null) ? null : new DBProperty(classProperty, database, target);
	}

	/**
	 * Gets the property by its java property name.
	 * <p> Only provides access to properties annotated with {@code DBColumn}.
	 * 
	 * @param propertyName
	 * @return
	 */
	public DBProperty getPropertyByName(String propertyName) {
		DBPropertyDefinition classProperty = classWrapper.getPropertyByName(propertyName);
		return (classProperty == null) ? null : new DBProperty(classProperty, database, target);
	}

	/**
	 * Gets all properties that are annotated with {@code DBColumn}.
	 * This method is intended for where you need to get/set property values
	 * on all properties in the class.
	 * 
	 * <p> Note: if you wish to iterate over the properties and only
	 * use their definitions (ie: meta-information), this method is not efficient.
	 * Use {@link #getPropertyDefinitions()} instead in that case.
	 * @return
	 */
	public List<DBProperty> getDBProperties() {
		List<DBProperty> list = new ArrayList<DBProperty>();
		for (DBPropertyDefinition classProperty: classWrapper.getProperties()) {
			list.add(new DBProperty(classProperty, database, target));
		}
		return list;
	}
	
	/**
	 * Gets all property definitions that are annotated with {@code DBColumn}.
	 * This method is intended for where you need to examine meta-information
	 * about all properties in a class.
	 * 
	 * <p> If you wish to get/set property values while iterating over the properties,
	 * use {@link #getDBProperties()} instead.
	 * @return
	 */
	public List<DBPropertyDefinition> getPropertyDefinitions() {
		return classWrapper.getProperties();
	}

// shouldn't be needed
//	/**
//	 * Gets the {@link DBTableName} annotation on the class, if it exists.
//	 * @return the annotation or null
//	 */
//	public DBTableName getDBTableNameAnnotation() {
//		return classAdaptor.getDBTableNameAnnotation();
//	}
}
