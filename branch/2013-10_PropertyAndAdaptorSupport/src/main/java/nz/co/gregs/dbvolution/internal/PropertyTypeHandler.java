package nz.co.gregs.dbvolution.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import nz.co.gregs.dbvolution.DBPebkacException;
import nz.co.gregs.dbvolution.DBRuntimeException;
import nz.co.gregs.dbvolution.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.DBTypeAdaptor;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with the type of a property.
 * This includes processing of the {@link DBAdaptType} annotation on a property,
 * and type conversion of the property's underlying type.
 * 
 * <p> This class handles the majority of the type support logic that is
 * exposed by the {@link DBPropertyDefinition} class, which just delegates to this class.
 * 
 * <p> This class behaves correctly when no {@link DBAdaptType} property is present.
 * @author Malcolm Lett
 */
// TODO: this class could also handle implicit type adaptors where the target object's properties
// are simple types, and we need to automatically convert between DBv data types.
class PropertyTypeHandler {
	private JavaProperty javaProperty;
	private Class<? extends QueryableDatatype> dbvPropertyType;
	private DBTypeAdaptor<Object,QueryableDatatype> typeAdaptor;
	private DBAdaptType annotation;

	/**
	 * 
	 * @param javaProperty the annotated property
	 */
	@SuppressWarnings("unchecked")
	public PropertyTypeHandler(JavaProperty javaProperty) {
		this.annotation = javaProperty.getAnnotation(DBAdaptType.class);
		if (annotation != null) {
			this.typeAdaptor = newTypeAdaptorInstanceGiven(javaProperty, annotation);
			this.dbvPropertyType = annotation.type();
		}
		else {
			if (!QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
				throw new DBPebkacException("Property "+javaProperty.qualifiedName()+" is not a supported type. "+
						"Use one of the standard DB types, or use the @"+DBAdaptType.class.getSimpleName()+" annotation "+
						"to adapt from a non-standard type.");
			}
			this.dbvPropertyType = (Class<? extends QueryableDatatype>)javaProperty.type();
		}
	}
	
	public void checkForErrors() throws DBPebkacException {
		// TODO: check that either:
		//        1) no type adaptor is present and the java property's type as a QueryableDatatype, or
		//        2) a type adaptor is present and the adapt type annotation indicates a QeueryableData type as the output type,
		//           and that the type adaptor's types are correct.
	}
	
	/**
	 * Gets the DBv-centric type of the property, possibly after type adaption.
	 */
	public Class<? extends QueryableDatatype> getType() {
		return dbvPropertyType;
	}
	
	/**
	 * Indicates whether the property's type is adapted by
	 * an explicit or implicit type adaptor.
	 * (Note: at present there is no support for implicit type adaptors)
	 * @return
	 */
	public boolean isTypeAdapted() {
		return (annotation != null);
	}

	/**
	 * Gets the annotation, if present.
	 * @return
	 */
	public DBAdaptType getDBTypeAdaptorAnnotation() {
		return annotation;
	}
	
	/**
	 * Gets the DBv-centric value from the underlying java property, converting if needed.
	 * This method behaves correctly regardless of whether an {@link DBAdaptType} annotation
	 * is present.
	 * @param target object containing the property
	 * @return the DBv-centric property value
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 * @throws IllegalStateException if the underlying java property is not readable
	 */
	public QueryableDatatype getDBvValue(Object target) {
		// get directly without type adaptor
		// TODO: what type checking should be performed here?
		if (typeAdaptor == null) {
			return (QueryableDatatype)javaProperty.get(target);
		}
		
		// set via type adaptor
		// TODO: what type checking should be performed here?
		else {
			Object value = javaProperty.get(target);
			
			try {
				return typeAdaptor.toDBvValue(value);
			} catch (RuntimeException e) {
				throw new DBThrownByEndUserCodeException("Type adaptor threw "+e.getClass().getSimpleName()+
						" when getting property "+javaProperty.qualifiedName()+": "+e.getLocalizedMessage(), e);
			}
		}
	}
	
	/**
	 * Sets the underlying java property according to the given DBv-centric value.
	 * This method behaves correctly regardless of whether an {@link DBAdaptType} annotation
	 * is present.
	 * @param target object containing the property
	 * @param dbvValue
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 * @throws IllegalStateException if the underlying java property is not writable
	 */
	public void setObjectValue(Object target, QueryableDatatype dbvValue) {
		// set directly without type adaptor
		// TODO: what type checking should be performed here?
		if (typeAdaptor == null) {
			javaProperty.set(target, dbvValue);
		}
		
		// set via type adaptor
		// TODO: what type checking should be performed here?
		else {
			Object value;
			try {
				value = typeAdaptor.toObjectValue(dbvValue);
			} catch (RuntimeException e) {
				throw new DBThrownByEndUserCodeException("Type adaptor threw "+e.getClass().getSimpleName()+
						" when setting property "+javaProperty.qualifiedName()+": "+e.getLocalizedMessage(), e);
			}
			
			javaProperty.set(target, value);
		}
	}
	
	/**
	 * Constructs a new instanceof the type adaptor referenced by the given annotation instance.
	 * Handles all exceptions and throws them as the appropriate runtime exceptions
	 * @param property
	 * @param annotation
	 * @return
	 * @throws DBRuntimeException on unexpected internal errors, and 
	 * @throws DBPebkacException on errors with the end-user supplied code
	 */
	private static DBTypeAdaptor<Object,QueryableDatatype> newTypeAdaptorInstanceGiven(JavaProperty property, DBAdaptType annotation) {
		Class<? extends DBTypeAdaptor<?,? extends QueryableDatatype>> adaptorClass = annotation.adaptor();
		if (adaptorClass == null) {
			// shouldn't be possible
			throw new DBRuntimeException("Encountered unexpected null "+DBAdaptType.class.getSimpleName()+
					".adptor() (probably a bug in DBvolution)");
		}
		
		// get default constructor
		Constructor<? extends DBTypeAdaptor<?,? extends QueryableDatatype>> constructor;
		try {
			constructor = adaptorClass.getConstructor();
		} catch (NoSuchMethodException e) {
			throw new DBPebkacException(adaptorClass.getName()+" has no default constructor, referenced by property "+
					property.qualifiedName(), e);
		} catch (SecurityException e) {
			// caused by a Java security manager or an attempt to access a non-visible field
			// without first making it visible
			throw new DBRuntimeException("Java security error retrieving constructor for "+adaptorClass.getName()+
					", referenced by property "+property.qualifiedName()+": "+e.getLocalizedMessage(), e);
		}
		
		// construct adaptor instance
		DBTypeAdaptor<?,? extends QueryableDatatype> instance;
		try {
			instance = constructor.newInstance();
		} catch (InstantiationException e) {
			throw new DBPebkacException(adaptorClass.getName()+" cannot be constructed (it is probably abstract), referenced by property "+
					property.qualifiedName(), e);
		} catch (IllegalAccessException e) {
			// caused by a Java security manager or an attempt to access a non-visible field
			// without first making it visible
			throw new DBRuntimeException("Java security error instantiating "+adaptorClass.getName()+
					", referenced by property "+property.qualifiedName()+": "+e.getLocalizedMessage(), e);
		} catch (IllegalArgumentException e) {
			// expected, so probably represents a bug
			throw new IllegalArgumentException("Internal error instantiating "+
					adaptorClass.getName()+", referenced by property "+property.qualifiedName()+": "+e.getLocalizedMessage(), e);
		} catch (InvocationTargetException e) {
			// any checked or runtime exception thrown by the setter method itself
			// TODO: check that this exception wraps runtime exceptions as well
			Throwable cause = e.getCause();
			throw new DBThrownByEndUserCodeException("Constructor threw "+cause.getClass().getSimpleName()+" when instantiating "+
					adaptorClass.getName()+", referenced by property "+property.qualifiedName()+": "+cause.getLocalizedMessage(), cause);
		}
		
		// downcast
		// (technically the instance is for <?,? extends QueryableDataType> but
		//  that can't be used reflectively when all we know is Object and QueryableDataType)
		@SuppressWarnings("unchecked")
		DBTypeAdaptor<Object,QueryableDatatype> result = (DBTypeAdaptor<Object,QueryableDatatype>)instance;
		return result;
	}

}
