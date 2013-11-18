package nz.co.gregs.dbvolution.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Date;

import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.datatypes.SimpleValueQueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.internal.InterfaceInfo.ParameterBounds;
import nz.co.gregs.dbvolution.internal.InterfaceInfo.UnsupportedType;

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

//    private static Log logger = LogFactory.getLog(PropertyTypeHandler.class);
    private final JavaProperty javaProperty;
    private final Class<? extends QueryableDatatype> dbvPropertyType;
    private final DBTypeAdaptor<Object, Object> typeAdaptor;
    private final QueryableDatatypeSyncer internalQdtSyncer;
    private final boolean identityOnly;
    private final DBAdaptType annotation;
    private static Class<?>[] SUPPORTED_SIMPLE_TYPES = {
        String.class,
        boolean.class, int.class, long.class, float.class, double.class,
        Boolean.class, Integer.class, Long.class, Float.class, Double.class
    };

    /**
     *
     * @param javaProperty the annotated property
     */
    @SuppressWarnings("unchecked")
    public PropertyTypeHandler(JavaProperty javaProperty, boolean processIdentityOnly) {
        this.javaProperty = javaProperty;
    	this.identityOnly = processIdentityOnly;
    	this.annotation = javaProperty.getAnnotation(DBAdaptType.class);

        Class<?> typeAdaptorInternalType = null; // DBv-internal
        Class<?> typeAdaptorExternalType = null;

        // validation: java property type must be a QueryableDataType if not using type adaptor
        if (annotation == null) {
            if (!QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
                throw new DBPebkacException(javaProperty.type().getName() + " is not a supported type on "
                        + javaProperty.qualifiedName() + ". "
                        + "Use one of the standard DB types, or use the @" + DBAdaptType.class.getSimpleName() + " annotation "
                        + "to adapt from a non-standard type.");
            }
        }

        // validation: type adaptor must implement TypeAdaptor interface if used
        if (annotation != null) {
            Class<?> typeAdaptorClass = annotation.value();
            if (!DBTypeAdaptor.class.isAssignableFrom(typeAdaptorClass)) {
                throw new DBPebkacException("Type adaptor " + typeAdaptorClass.getName() + " must implement "
                        + DBTypeAdaptor.class.getSimpleName() + ", on " + javaProperty.qualifiedName());
            }
        }

        // validation: type adaptor must not be an interface or abstract
        if (annotation != null) {
            Class<?> typeAdaptorClass = annotation.value();
            if (typeAdaptorClass.isInterface()) {
                throw new DBPebkacException("Type adaptor " + typeAdaptorClass.getName()
                        + " must not be an interface, on " + javaProperty.qualifiedName());
            }
            if (Modifier.isAbstract(typeAdaptorClass.getModifiers())) {
                throw new DBPebkacException("Type adaptor " + typeAdaptorClass.getName()
                        + " must not be abstract, on " + javaProperty.qualifiedName());
            }
        }

        // validation: type adaptor must use only acceptable styles of generics
        // (note: rule de-activates if InterfaceInfo can't handle the class,
        //   or if other assumptions are broken.
        //   This is intentional to future-proof and because generics of type
        //   hierarchies is tremendously complex and its process very prone to error.)
        if (annotation != null) {
            Class<?> typeAdaptorClass = annotation.value();
            ParameterBounds[] parameterBounds = null;
            try {
                InterfaceInfo interfaceInfo = new InterfaceInfo(DBTypeAdaptor.class, typeAdaptorClass);
                parameterBounds = interfaceInfo.getInterfaceParameterValueBounds();
            } catch (UnsupportedOperationException dropped) {
                // bumped into generics that can't be handled, so best to give the
                // end-user the benefit of doubt and just skip the validation
//                logger.debug("Cancelled validation on type adaptor " + typeAdaptorClass.getName()
//                        + " due to internal error: " + dropped.getMessage(), dropped);
            }
            if (parameterBounds != null && parameterBounds.length == 2) {
                if (parameterBounds[0].isUpperMulti()) {
                    throw new DBPebkacException("Type adaptor " + typeAdaptorClass.getName() + " must not be "
                            + "declared with multiple super types for type variables, "
                            + "on " + javaProperty.qualifiedName());
                }
                if (parameterBounds[1].isUpperMulti()) {
                    throw new DBPebkacException("Type adaptor " + typeAdaptorClass.getName() + " must not be "
                            + "declared with multiple super types for type variables, "
                            + "on " + javaProperty.qualifiedName());
                }

                try {
                    typeAdaptorExternalType = parameterBounds[0].upperClass();
                } catch (UnsupportedType e) {
                    // rules dependent on this attribute will be disabled
                }

                try {
                    typeAdaptorInternalType = parameterBounds[1].upperClass();
                } catch (UnsupportedType e) {
                    // rules dependent on this attribute will be disabled
                }
            }
        }

        // validation: type adaptor's external type must be compatible with simple-type java property
        if (typeAdaptorExternalType != null && !QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
            if (!typeAdaptorExternalType.isAssignableFrom(javaProperty.type())) {
                throw new DBPebkacException(
                        "Type adaptor " + annotation.value().getSimpleName() + " is not compatible "
                        + " with " + javaProperty.type().getName() + ", on " + javaProperty.qualifiedName());
            }
        }

        // validation: type adaptor's external type must be compatible with actual QDT java property
        if (typeAdaptorExternalType != null && QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
            // TODO
        }

        // validation: type adaptor's internal type must be supported simple type if no explicit type
        if (typeAdaptorInternalType != null && explicitTypeOrNullOf(annotation) == null) {
            boolean supported = false;
            for (Class<?> simpleType: SUPPORTED_SIMPLE_TYPES) {
                if (simpleType.isAssignableFrom(typeAdaptorInternalType)) {
                    supported = true;
                }
            }
            if (!supported) {
                throw new DBPebkacException(
                        "Type adaptor " + annotation.value().getName() + " internal type "
                        + typeAdaptorInternalType.getSimpleName() + " is not supported, on "
                        + javaProperty.qualifiedName());
            }
        }

        // validation: explicit type must be given if type adaptor's internal type isn't one where
        //             implied internal type is supported
        if (typeAdaptorInternalType != null && explicitTypeOrNullOf(annotation) == null) {
            if (inferredQDTTypeForSimpleType(typeAdaptorInternalType) == null) {
                throw new DBPebkacException(
                        "Must specify internal type when adapting to type " + typeAdaptorInternalType.getName()
                        + ", on " + javaProperty.qualifiedName());
            }
        }

        // validation: type adaptor's internal type be compatible with explicit type if specified
        if (typeAdaptorInternalType != null && explicitTypeOrNullOf(annotation) != null) {
            // TODO
        }

        // validation: explicitly declared type adapted target DBv property type must be a QueryableDataType
        if (annotation != null && explicitTypeOrNullOf(annotation) != null) {
            if (!QueryableDatatype.class.isAssignableFrom(explicitTypeOrNullOf(annotation))) {
                throw new DBPebkacException("@DB" + DBAdaptType.class.getSimpleName() + "(type) on "
                        + javaProperty.qualifiedName() + " is not a supported type. "
                        + "Use one of the standard DB types, or use the @" + DBAdaptType.class.getSimpleName() + " annotation "
                        + "to adapt from a non-standard type.");
            }
        }

        // populate everything
        if (annotation == null) {
        	// populate when no annotation
        	this.typeAdaptor = null;
            this.dbvPropertyType = (Class<? extends QueryableDatatype>) javaProperty.type();
            this.internalQdtSyncer = null;
        }
        else if (identityOnly) {
        	// populate identity-only information when type adaptor declared
            Class<? extends QueryableDatatype> type;
            type = explicitTypeOrNullOf(annotation);
            if (type == null && typeAdaptorInternalType != null) {
                type = inferredQDTTypeForSimpleType(typeAdaptorInternalType);
            }
            if (type == null) {
                throw new NullPointerException("null dbvPropertyType, this is an internal bug");
            }
            this.dbvPropertyType = type;

        	this.typeAdaptor = null;
            this.internalQdtSyncer = null;
        }
        else {
        	// initialise type adapting
            this.typeAdaptor = newTypeAdaptorInstanceGiven(javaProperty, annotation);
            
            Class<? extends QueryableDatatype> type;
            type = explicitTypeOrNullOf(annotation);
            if (type == null && typeAdaptorInternalType != null) {
                type = inferredQDTTypeForSimpleType(typeAdaptorInternalType);
            }
            if (type == null) {
                throw new NullPointerException("null dbvPropertyType, this is an internal bug");
            }
            this.dbvPropertyType = type;

            if (QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
                this.internalQdtSyncer = new QueryableDatatypeSyncer(javaProperty.qualifiedName(),
                        this.dbvPropertyType, this.typeAdaptor);
            } else {
                this.internalQdtSyncer = new SimpleValueQueryableDatatypeSyncer(javaProperty.qualifiedName(),
                        this.dbvPropertyType, this.typeAdaptor);
            }
        }
    }

    private static Class<? extends QueryableDatatype> inferredQDTTypeForSimpleType(Class<?> simpleType) {
        if (simpleType.equals(String.class)) {
            return DBString.class;
        } else if (Number.class.isAssignableFrom(simpleType)) {
            if (Integer.class.isAssignableFrom(simpleType) || Long.class.isAssignableFrom(simpleType)) {
                return DBInteger.class;
            }
            if (Float.class.isAssignableFrom(simpleType) || Double.class.isAssignableFrom(simpleType)) {
                return DBNumber.class;
            } else {
                return DBNumber.class;
            }
        } else if (Date.class.isAssignableFrom(simpleType)) {
            return DBDate.class;
        }

        // all remaining types require explicit declaration
        return null;
    }

    /**
     * Internal helper to support the way annotation attribute defaulting works.
     *
     * @param annotation
     * @return
     */
    private static Class<? extends QueryableDatatype> explicitTypeOrNullOf(DBAdaptType annotation) {
        if (annotation == null) {
            return null;
        }

        // detect default
        if (annotation.type().equals(QueryableDatatype.class)) {
            return null;
        }

        // return value
        return annotation.type();
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
    	if (identityOnly) {
    		throw new AssertionError("Attempt to access non-identity information of identity-only property type handler");
    	}
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
    public QueryableDatatype getJavaPropertyAsQueryableDatatype(Object target) {
    	if (identityOnly) {
    		throw new AssertionError("Attempt to read value from identity-only property");
    	}
    	
        // get via type adaptor and simple-type java property
        if (typeAdaptor != null && internalQdtSyncer instanceof SimpleValueQueryableDatatypeSyncer) {
            SimpleValueQueryableDatatypeSyncer syncer = (SimpleValueQueryableDatatypeSyncer) internalQdtSyncer;
            Object externalValue = javaProperty.get(target);

            // convert
            // TODO think this still needs some last-minute type checks
            return syncer.setInternalQDTFromExternalSimpleValue(externalValue);
        } // get via type adaptor and QDT java property
        else if (typeAdaptor != null) {
            Object externalValue = javaProperty.get(target);

            // this should be completely safe by now
            QueryableDatatype externalQdt = (QueryableDatatype) externalValue;

            // convert
            return internalQdtSyncer.setInternalQDTFromExternalQDT(externalQdt);
        } // get directly without type adaptor
        // (note: type checking was performed at creation time)
        else {
            return (QueryableDatatype) javaProperty.get(target);
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
    public void setJavaPropertyAsQueryableDatatype(Object target, QueryableDatatype dbvValue) {
    	if (identityOnly) {
    		throw new AssertionError("Attempt to write value to identity-only property");
    	}
    	
        // set via type adaptor and simple-type java property
        if (typeAdaptor != null && internalQdtSyncer instanceof SimpleValueQueryableDatatypeSyncer) {
            SimpleValueQueryableDatatypeSyncer syncer = (SimpleValueQueryableDatatypeSyncer) internalQdtSyncer;
            syncer.setInternalQueryableDatatype(dbvValue);
            Object externalValue = syncer.getExternalSimpleValueFromInternalQDT();

            // TODO think this still needs some last-minute type checks
            javaProperty.set(target, externalValue);
        } // set via type adaptor and QDT java property
        else if (typeAdaptor != null) {
            Object externalValue = javaProperty.get(target);

            // this should be completely safe by now
            QueryableDatatype externalQdt = (QueryableDatatype) externalValue;

            // convert
            internalQdtSyncer.setInternalQueryableDatatype(dbvValue);
            externalQdt = internalQdtSyncer.setExternalFromInternalQDT(externalQdt);
            if (externalQdt == null && externalValue != null) {
            	javaProperty.set(target, null);
            }
        } // set directly without type adaptor
        // (note: type checking was performed at creation time)
        else {
            javaProperty.set(target, dbvValue);
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
    private static DBTypeAdaptor<Object, Object> newTypeAdaptorInstanceGiven(JavaProperty property, DBAdaptType annotation) {
        Class<? extends DBTypeAdaptor<?, ?>> adaptorClass = annotation.value();
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

        try {
            adaptorClass.newInstance();
        } catch (InstantiationException e) {
            throw new DBPebkacException("Type adaptor " + adaptorClass.getName()
                    + " could not be constructed, on property "
                    + property.qualifiedName() + ": " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new DBPebkacException("Type adaptor " + adaptorClass.getName()
                    + " could not be constructed, on property "
                    + property.qualifiedName() + ": " + e.getMessage(), e);
        }

        // get default constructor
        Constructor<? extends DBTypeAdaptor<?, ?>> constructor;
        try {
            constructor = adaptorClass.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new DBPebkacException("Type adaptor " + adaptorClass.getName()
                    + " has no default constructor, on property "
                    + property.qualifiedName(), e);
        } catch (SecurityException e) {
            // caused by a Java security manager or an attempt to access a non-visible field
            // without first making it visible
            throw new DBRuntimeException("Java security error retrieving constructor for " + adaptorClass.getName()
                    + ", referenced by property " + property.qualifiedName() + ": " + e.getLocalizedMessage(), e);
        }

        // construct adaptor instance
        DBTypeAdaptor<?, ?> instance;
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
            Throwable cause = (e.getCause() == null) ? e : e.getCause();
			String msg = (cause.getLocalizedMessage() == null) ? "" : ": "+cause.getLocalizedMessage();
            throw new DBThrownByEndUserCodeException("Constructor threw " + cause.getClass().getSimpleName() + " when instantiating "
                    + adaptorClass.getName() + ", referenced by property " + property.qualifiedName() + msg, cause);
        }

        // downcast
        // (technically the instance is for <?,? extends QueryableDataType> but
        //  that can't be used reflectively when all we know is Object and QueryableDataType)
        @SuppressWarnings("unchecked")
        DBTypeAdaptor<Object, Object> result = (DBTypeAdaptor<Object, Object>) instance;
        return result;
    }
//    private abstract class InternalQDTWrapper {
//    	public InternalQDTWrapper() {
//    	}
//    	
//    	protected DBTypeAdaptor<Object,Object> typeAdaptor() {
//    		return typeAdaptor;
//    	}
//    	
//    	//public abstract void setInternal
//    	
//    	public abstract QueryableDatatype getQueryableDatatype();
//    }
//    private class InternalDBNumberWrapper extends InternalQDTWrapper {
//    	private DBNumber internalQdt = new DBNumber();
//    	
//		@Override
//		public DBNumber getQueryableDatatype() {
//			return internalQdt;
//		}
//    }
}
