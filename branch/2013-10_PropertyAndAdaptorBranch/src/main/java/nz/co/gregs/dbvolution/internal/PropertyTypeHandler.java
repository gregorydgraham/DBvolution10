package nz.co.gregs.dbvolution.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import nz.co.gregs.dbvolution.DBTypeAdaptor;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with the type of a property. This includes
 * processing of the {@link DBAdaptType} annotation on a property, and type
 * conversion of the property's underlying type.
 *
 * <p> This class handles the majority of the type support logic that is exposed
 * by the {@link DBPropertyDefinition} class, which just delegates to this
 * class.
 *
 * <p> This class behaves correctly when no {@link DBAdaptType} property is
 * present.
 *
 * @author Malcolm Lett
 */
// TODO: this class could also handle implicit type adaptors where the target object's properties
// are simple types, and we need to automatically convert between DBv data types.
class PropertyTypeHandler {

    private JavaProperty javaProperty;
    private Class<? extends QueryableDatatype> dbvPropertyType;
    private DBTypeAdaptor<Object, QueryableDatatype> typeAdaptor;
    private DBAdaptType annotation;

    /**
     *
     * @param javaProperty the annotated property
     */
    @SuppressWarnings("unchecked")
    public PropertyTypeHandler(JavaProperty javaProperty) {
        this.javaProperty = javaProperty;
        this.annotation = javaProperty.getAnnotation(DBAdaptType.class);

        if (annotation != null) {
            this.typeAdaptor = newTypeAdaptorInstanceGiven(javaProperty, annotation);
            this.dbvPropertyType = annotation.type(); // TODO: make this optional
        } else {
            // validation: java property type must be a QueryableDataType
            if (!QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
                throw new DBPebkacException("Property " + javaProperty.qualifiedName() + " is not a supported type. "
                        + "Use one of the standard DB types, or use the @" + DBAdaptType.class.getSimpleName() + " annotation "
                        + "to adapt from a non-standard type.");
            }
            this.dbvPropertyType = (Class<? extends QueryableDatatype>) javaProperty.type();
        }

        // validation: if using type adaptor, type adaptor must have acceptable types
        // TODO: ideal
//		if (typeAdaptor != null) {
//			if (dbvPropertyType == null) {
//				Method toDBvValueMethod = getMethodThatTakesType(javaProperty, typeAdaptor.getClass(), "toDBvValue",
//						javaProperty.type());
//				Class<?> dbvType = toDBvValueMethod.getReturnType();
//				
//				validateHasMethod(javaProperty, typeAdaptor.getClass(), "toObjectValue", dbvType, javaProperty.type());
//			}
//			else {
//				validateHasMethod(javaProperty, typeAdaptor.getClass(), "toObjectValue", dbvPropertyType, javaProperty.type());
//				validateHasMethod(javaProperty, typeAdaptor.getClass(), "toDBvValue", javaProperty.type(), dbvPropertyType);
//			}
//		}


//		if (typeAdaptor != null) {
//			try {
//				System.out.println("-------------------");
//				for (Method method: typeAdaptor.getClass().getMethods()) {
//					if (method.getName().equals("toObjectValue") || method.getName().equals("toDBvValue")) {
//						System.out.println(descriptionOf(method));
//					}
//				}

//				System.out.println("-------------------");
//				Method method = typeAdaptor.getClass().getMethod("toObjectValue", DBInteger.class);
//				System.out.println("synthetic="+method.isSynthetic()+", bridge="+method.isBridge()+" - "+method);
//				System.out.println("return type: "+method.getReturnType());
//				Object result = method.invoke(typeAdaptor, new DBInteger());
//				System.out.println("result="+result);
//	
//				System.out.println("-------------------");
//				method = typeAdaptor.getClass().getMethod("toObjectValue", QueryableDatatype.class);
//				System.out.println("synthetic="+method.isSynthetic()+", bridge="+method.isBridge()+" - "+method);
//				System.out.println("return type: "+method.getReturnType());
//				result = method.invoke(typeAdaptor, new DBInteger());
//				System.out.println("result="+result);
//			} catch (NoSuchMethodException e) {
//				throw new RuntimeException(e);
//			} catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//		}

    }

//	/**
//	 * Doesn't accept 
//	 * @param adaptorClass
//	 * @param requiredParameterType
//	 * @param requiredReturnType
//	 * @return
//	 */
//	protected void validateHasMethod(JavaProperty property, Class<?> adaptorClass, String requiredName, Class<?> requiredParameterType, Class<?> requiredReturnType) {
//		System.out.println("testing isCorrectToObjectValueMethod("+requiredParameterType.getSimpleName()+" -> "+requiredReturnType.getSimpleName()+")...");
//		List<Class<?>> possibleWrongParamTypes = new ArrayList<Class<?>>();
//		List<Class<?>> possibleWrongReturnTypes = new ArrayList<Class<?>>();
//		
//		for (Method method: adaptorClass.getMethods()) {
//			// ignore synthetic/bridge methods (which are the methods in the interface, I think)
//			if (!method.isSynthetic() && !method.isBridge() && method.getName().equals(requiredName)) {
//				if (method.getParameterTypes().length == 1 && method.getReturnType() != null) {
//					System.out.println("  "+descriptionOf(method));
//					boolean paramOK = false;
//					boolean returnOK = false;
//					
//					// must be able to assign from given type to parameter type
//					if (method.getParameterTypes()[0].isAssignableFrom(requiredParameterType)) {
//						paramOK = true;
//					}
//					else {
//						possibleWrongParamTypes.add(method.getParameterTypes()[0]);
//					}
//					
//					// must be able to assign from return type to desired type
//					if (requiredReturnType.isAssignableFrom(method.getReturnType())) {
//						returnOK = true;
//					}
//					else {
//						possibleWrongReturnTypes.add(method.getReturnType());
//					}
//					
//					if (paramOK && returnOK) {
//						return;
//					}
//				}
//			}
//		}
//		
//		// attempt to produce detailed error message
//		StringBuilder buf = new StringBuilder();
//		if ((!possibleWrongParamTypes.isEmpty() || !possibleWrongReturnTypes.isEmpty()) && 
//				possibleWrongParamTypes.size() <= 1 &&
//				possibleWrongReturnTypes.size() <= 1) {
//			buf.append(", got ");
//			if (!possibleWrongParamTypes.isEmpty()) {
//				buf.append(possibleWrongParamTypes.get(0).getSimpleName());
//			}
//			if (!possibleWrongReturnTypes.isEmpty()) {
//				if (buf.length() > 0) buf.append(" and ");
//				buf.append(possibleWrongReturnTypes.get(0).getSimpleName());
//			}
//		}
//		throw new DBPebkacException("TypeAdaptor converts between wrong types, expected "+
//				requiredParameterType.getSimpleName()+" and"+
//				requiredReturnType.getSimpleName()+
//				buf.toString()+
//				"on property "+property.qualifiedName());
//	}
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
     * Indicates whether the property's type is adapted by an explicit or
     * implicit type adaptor. (Note: at present there is no support for implicit
     * type adaptors)
     *
     * @return
     */
    public boolean isTypeAdapted() {
        return (annotation != null);
    }

    /**
     * Gets the annotation, if present.
     *
     * @return
     */
    public DBAdaptType getDBTypeAdaptorAnnotation() {
        return annotation;
    }

    /**
     * Gets the DBv-centric value from the underlying java property, converting
     * if needed. This method behaves correctly regardless of whether an
     * {@link DBAdaptType} annotation is present.
     *
     * @param target object containing the property
     * @return the DBv-centric property value
     * @throws DBThrownByEndUserCodeException if any user code throws an
     * exception
     * @throws IllegalStateException if the underlying java property is not
     * readable
     */
    public QueryableDatatype getDBvValue(Object target) {
        // get directly without type adaptor
        // TODO: what type checking should be performed here?
        if (typeAdaptor == null) {
            return (QueryableDatatype) javaProperty.get(target);
        } // set via type adaptor
        // TODO: what type checking should be performed here?
        else {
            Object value = javaProperty.get(target);

            try {
                return typeAdaptor.toDBvValue(value);
            } catch (RuntimeException e) {
                throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
                        + " when getting property " + javaProperty.qualifiedName() + ": " + e.getLocalizedMessage(), e);
            }
        }
    }

    /**
     * Sets the underlying java property according to the given DBv-centric
     * value. This method behaves correctly regardless of whether an
     * {@link DBAdaptType} annotation is present.
     *
     * @param target object containing the property
     * @param dbvValue
     * @throws DBThrownByEndUserCodeException if any user code throws an
     * exception
     * @throws IllegalStateException if the underlying java property is not
     * writable
     */
    public void setObjectValue(Object target, QueryableDatatype dbvValue) {
        // set directly without type adaptor
        // TODO: what type checking should be performed here?
        if (typeAdaptor == null) {
            javaProperty.set(target, dbvValue);
        } // set via type adaptor
        // TODO: what type checking should be performed here?
        else {
            Object value;
            try {
                value = typeAdaptor.toObjectValue(dbvValue);
            } catch (RuntimeException e) {
                throw new DBThrownByEndUserCodeException("Type adaptor threw " + e.getClass().getSimpleName()
                        + " when setting property " + javaProperty.qualifiedName() + ": " + e.getLocalizedMessage(), e);
            }

            javaProperty.set(target, value);
        }
    }

    /**
     * Constructs a new instanceof the type adaptor referenced by the given
     * annotation instance. Handles all exceptions and throws them as the
     * appropriate runtime exceptions
     *
     * @param property
     * @param annotation
     * @return
     * @throws DBRuntimeException on unexpected internal errors, and
     * @throws DBPebkacException on errors with the end-user supplied code
     */
    private static DBTypeAdaptor<Object, QueryableDatatype> newTypeAdaptorInstanceGiven(JavaProperty property, DBAdaptType annotation) {
        Class<? extends DBTypeAdaptor<?, ? extends QueryableDatatype>> adaptorClass = annotation.adaptor();
        if (adaptorClass == null) {
            // shouldn't be possible
            throw new DBRuntimeException("Encountered unexpected null " + DBAdaptType.class.getSimpleName()
                    + ".adptor() (probably a bug in DBvolution)");
        }

        if (adaptorClass.isInterface()) {
            throw new DBPebkacException("TypeAdaptor cannot be an interface (" + adaptorClass.getSimpleName()
                    + "), on property " + property.qualifiedName());
        }
        if (Modifier.isAbstract(adaptorClass.getModifiers())) {
            throw new DBPebkacException("TypeAdaptor cannot be an abstract class (" + adaptorClass.getSimpleName()
                    + "), on property " + property.qualifiedName());
        }

        // get default constructor
        Constructor<? extends DBTypeAdaptor<?, ? extends QueryableDatatype>> constructor;
        try {
            constructor = adaptorClass.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new DBPebkacException(adaptorClass.getName() + " has no default constructor, referenced by property "
                    + property.qualifiedName(), e);
        } catch (SecurityException e) {
            // caused by a Java security manager or an attempt to access a non-visible field
            // without first making it visible
            throw new DBRuntimeException("Java security error retrieving constructor for " + adaptorClass.getName()
                    + ", referenced by property " + property.qualifiedName() + ": " + e.getLocalizedMessage(), e);
        }

        // construct adaptor instance
        DBTypeAdaptor<?, ? extends QueryableDatatype> instance;
        try {
            instance = constructor.newInstance();
        } catch (InstantiationException e) {
            throw new DBPebkacException(adaptorClass.getName() + " cannot be constructed (it is probably abstract), referenced by property "
                    + property.qualifiedName(), e);
        } catch (IllegalAccessException e) {
            // caused by a Java security manager or an attempt to access a non-visible field
            // without first making it visible
            throw new DBRuntimeException("Java security error instantiating " + adaptorClass.getName()
                    + ", referenced by property " + property.qualifiedName() + ": " + e.getLocalizedMessage(), e);
        } catch (IllegalArgumentException e) {
            // expected, so probably represents a bug
            throw new IllegalArgumentException("Internal error instantiating "
                    + adaptorClass.getName() + ", referenced by property " + property.qualifiedName() + ": " + e.getLocalizedMessage(), e);
        } catch (InvocationTargetException e) {
            // any checked or runtime exception thrown by the setter method itself
            // TODO: check that this exception wraps runtime exceptions as well
            Throwable cause = e.getCause();
            throw new DBThrownByEndUserCodeException("Constructor threw " + cause.getClass().getSimpleName() + " when instantiating "
                    + adaptorClass.getName() + ", referenced by property " + property.qualifiedName() + ": " + cause.getLocalizedMessage(), cause);
        }

        // downcast
        // (technically the instance is for <?,? extends QueryableDataType> but
        //  that can't be used reflectively when all we know is Object and QueryableDataType)
        @SuppressWarnings("unchecked")
        DBTypeAdaptor<Object, QueryableDatatype> result = (DBTypeAdaptor<Object, QueryableDatatype>) instance;
        return result;
    }
}
