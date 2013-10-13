package nz.co.gregs.dbvolution.internal;

import java.util.ArrayList;
import java.util.List;

import nz.co.gregs.dbvolution.DBPebkacException;
import nz.co.gregs.dbvolution.DBRuntimeException;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * Wraps a specific target object according to its type's {@link ClassAdaptor}.
 * To create instances of this type, call {@link ClassAdaptor#objectAdaptorFor(DBDefinition, Object)}
 * on the appropriate {@link ClassAdaptor}. 
 * 
 * <p> Instances of this class are lightweight and efficient to create,
 * and they are intended to be short lived.
 * Instances of this class must not be shared between different DBDatabase instances,
 * however they can be safely associated within a single DBDatabase instance.
 * 
 * <p> Instances of this class are <i>thread-safe</i>.
 * @author Malcolm Lett
 */
//TODO: consider whether this should be called ObjectWrapper to avoid overload of term "Adaptor"
public class ObjectAdaptor {
	private final DBDefinition dbDefn;
	private final ClassAdaptor classAdaptor;
	private final Object target;
	
	/**
	 * Called by {@link ClassAdaptor#objectAdaptorFor(DBDefinition, Object)}.
	 * @param dbDefn
	 * @param classAdaptor
	 * @param target the target object of the same type as analysed by {@code classAdaptor}
	 */
	ObjectAdaptor(DBDefinition dbDefn, ClassAdaptor classAdaptor, Object target) {
		if (target == null) {
			throw new DBRuntimeException("Target object is null");
		}
		if (!classAdaptor.adaptee().isInstance(target)) {
			throw new DBRuntimeException("Target object's type ("+target.getClass().getName()+
					") is not compatible with given class adaptor for type "+classAdaptor.adaptee().getName()+
					" (this is probably a bug in DBvolution)");
		}
		
		this.dbDefn = dbDefn;
		this.target = target;
		this.classAdaptor = classAdaptor;
	}

	/**
	 * Checks all the annotations etc. and errors.
	 * @throws Exception if has any errors
	 */
	public void checkForErrors() throws DBPebkacException {
		classAdaptor.checkForErrors();
	}
	
	/**
	 * Gets a string representation suitable for debugging.
	 */
	@Override
	public String toString() {
		if (isTable()) {
			return "ObjectAdapter<"+tableName()+":"+classAdaptor.adaptee().getName()+">";
		}
		else {
			return "ObjectAdapter<no-table:"+classAdaptor.adaptee().getName()+">";
		}
	}
	
	/**
	 * Gets the wrapped object type supported by this {@code ObjectAdaptor}.
	 * Note: this should be the same as the wrapped object's actual type.
	 * @return
	 */
	public Class<?> adapteeType() {
		return classAdaptor.adaptee();
	}
	
	/**
	 * Gets the object wrapped by this {@code ObjectAdaptor}.
	 * @return
	 */
	public Object adapteeObject() {
		return target;
	}
	
	/**
	 * Indicates whether this class maps to a database column.
	 * @return
	 */
	public boolean isTable() {
		return classAdaptor.isTable();
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
		return classAdaptor.tableName();
	}
	
	/**
	 * Gets the properties that together form the primary key, if any are marked.
	 * In most tables this will be exactly one property.
	 * @return the non-empty list of properties, or null if no primary key
	 */
	public List<ClassDBProperty> primaryKey() {
		return classAdaptor.primaryKey();
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
	public DBProperty getPropertyByColumn(String columnName) {
		ClassDBProperty classProperty = classAdaptor.getPropertyByColumn(dbDefn, columnName);
		return (classProperty == null) ? null : new DBProperty(classProperty, dbDefn, target);
	}

	/**
	 * Gets the property by its java property name.
	 * <p> Only provides access to properties annotated with {@code DBColumn}.
	 * 
	 * @param propertyName
	 * @return
	 */
	public DBProperty getPropertyByName(String propertyName) {
		ClassDBProperty classProperty = classAdaptor.getPropertyByName(propertyName);
		return (classProperty == null) ? null : new DBProperty(classProperty, dbDefn, target);
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
	public List<DBProperty> getProperties() {
		List<DBProperty> list = new ArrayList<DBProperty>();
		for (ClassDBProperty classProperty: classAdaptor.getProperties()) {
			list.add(new DBProperty(classProperty, dbDefn, target));
		}
		return list;
	}
	
	/**
	 * Gets all property definitions that are annotated with {@code DBColumn}.
	 * This method is intended for where you need to examine meta-information
	 * about all properties in a class.
	 * 
	 * <p> If you wish to get/set property values while iterating over the properties,
	 * use {@link #getProperties()} instead.
	 * @return
	 */
	public List<ClassDBProperty> getPropertyDefinitions() {
		return classAdaptor.getProperties();
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
