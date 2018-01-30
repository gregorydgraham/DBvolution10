package nz.co.gregs.dbvolution.internal.properties;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Date;

import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBStringTrimmed;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.datatypes.DBUnknownDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.datatypes.SimpleValueQueryableDatatypeSyncer;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.DBThrownByEndUserCodeException;
import nz.co.gregs.dbvolution.exceptions.InvalidDeclaredTypeException;
import nz.co.gregs.dbvolution.internal.properties.InterfaceInfo.ParameterBounds;
import nz.co.gregs.dbvolution.internal.properties.InterfaceInfo.UnsupportedType;

/**
 * Handles annotation processing, business logic, validation rules, defaulting,
 * and error handling associated with the type of a property. This includes
 * processing of the {@link DBAdaptType} annotation on a property, and type
 * conversion of the property's underlying type.
 *
 * <p>
 * This class handles the majority of the type support logic that is exposed by
 * the {@link DBPropertyDefinition} class, which just delegates to this class.
 *
 * <p>
 * This class behaves correctly when no {@link DBAdaptType} property is present.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Malcolm Lett
 */
// TODO: this class could also handle implicit type adaptors where the target object's properties
// are simple types, and we need to automatically convert between DBv data types.
class PropertyTypeHandler implements Serializable{

	private static final long serialVersionUID = 1l;

	private final JavaProperty javaProperty;
	private final Class<?> genericPropertyType;
	private final Class<? extends QueryableDatatype<?>> dbvPropertyType;
	private transient final DBTypeAdaptor<Object, Object> typeAdaptor;
	private final QueryableDatatypeSyncer internalQdtSyncer;
	private final boolean identityOnly;
	private transient final DBAdaptType dbAdaptTypeAnnotation;
	private transient final DBColumn dbColumnAnnotation;

	/**
	 *
	 * @param javaProperty the annotated property
	 */
	@SuppressWarnings("unchecked")
	PropertyTypeHandler(JavaProperty javaProperty, boolean processIdentityOnly) {
		this.javaProperty = javaProperty;
		this.identityOnly = processIdentityOnly;
		this.dbAdaptTypeAnnotation = javaProperty.getAnnotation(DBAdaptType.class);
		this.dbColumnAnnotation = javaProperty.getAnnotation(DBColumn.class);
		boolean isColumn = (dbColumnAnnotation != null);

		Class<?> typeAdaptorClass = null;
		if (dbAdaptTypeAnnotation != null) {
			typeAdaptorClass = dbAdaptTypeAnnotation.value();
		}
		Class<?> typeAdaptorInternalType = null; // DBv-internal
		Class<?> typeAdaptorExternalType = null;

		// validation: must use type adaptor if java property not a QueryableDataType
		if (isColumn && !QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
			if (dbAdaptTypeAnnotation == null) {
				throw new InvalidDeclaredTypeException(javaProperty.type().getName() + " is not a supported type on " + javaProperty + ". "
						+ "Use one of the standard DB types, or use the @" + DBAdaptType.class.getSimpleName() + " annotation "
						+ "to adapt from a non-standard type.");
			}
		}

		// validation: type adaptor must implement TypeAdaptor interface if used
		if (typeAdaptorClass != null) {
			if (!DBTypeAdaptor.class.isAssignableFrom(typeAdaptorClass)) {
				throw new InvalidDeclaredTypeException("Type adaptor " + typeAdaptorClass.getName() + " must implement "
						+ DBTypeAdaptor.class.getSimpleName() + ", on " + javaProperty);
			}
		}

		// validation: type adaptor must not be an interface or abstract
		if (typeAdaptorClass != null) {
			if (typeAdaptorClass.isInterface()) {
				throw new InvalidDeclaredTypeException("Type adaptor " + typeAdaptorClass.getName()
						+ " must not be an interface, on " + javaProperty);
			}
			if (Modifier.isAbstract(typeAdaptorClass.getModifiers())) {
				throw new InvalidDeclaredTypeException("Type adaptor " + typeAdaptorClass.getName()
						+ " must not be abstract, on " + javaProperty);
			}
		}

		// validation: type adaptor must use only acceptable styles of generics
		// (note: rule de-activates if InterfaceInfo can't handle the class,
		//   or if other assumptions are broken.
		//   This is intentional to future-proof and because generics of type
		//   hierarchies is tremendously complex and its process very prone to error.)
		if (typeAdaptorClass != null) {
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
					throw new InvalidDeclaredTypeException("Type adaptor " + typeAdaptorClass.getName() + " must not be"
							+ " declared with multiple super types for type variables"
							+ ", on " + javaProperty);
				}
				if (parameterBounds[1].isUpperMulti()) {
					throw new InvalidDeclaredTypeException("Type adaptor " + typeAdaptorClass.getName() + " must not be"
							+ " declared with multiple super types for type variables"
							+ ", on " + javaProperty);
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

		// validation: Type adaptor's external type must not be a QDT.
		if (typeAdaptorExternalType != null) {
			if (QueryableDatatype.class.isAssignableFrom(typeAdaptorExternalType)) {
				throw new InvalidDeclaredTypeException(
						"Type adaptor's external type must not be a " + QueryableDatatype.class.getSimpleName()
						+ ", on " + javaProperty);
			}
		}

		// validation: Type adaptor's internal type must not be a QDT.
		if (typeAdaptorInternalType != null) {
			if (QueryableDatatype.class.isAssignableFrom(typeAdaptorInternalType)) {
				throw new InvalidDeclaredTypeException(
						"Type adaptor's internal type must not be a " + QueryableDatatype.class.getSimpleName()
						+ ", on " + javaProperty);
			}
		}

		// validation: explicit external type must be a QDT and must not be abstract or an interface
		if (dbAdaptTypeAnnotation != null && explicitTypeOrNullOf(dbAdaptTypeAnnotation) != null) {
			Class<?> explicitQDTType = explicitTypeOrNullOf(dbAdaptTypeAnnotation);
			if (!QueryableDatatype.class.isAssignableFrom(explicitQDTType)) {
				throw new InvalidDeclaredTypeException("@DB" + DBAdaptType.class.getSimpleName() + "(type) on "
						+ javaProperty + " is not a supported type. "
						+ "Use one of the standard DB types.");
			}
			if (Modifier.isAbstract(explicitQDTType.getModifiers()) || Modifier.isInterface(explicitQDTType.getModifiers())) {
				throw new InvalidDeclaredTypeException("@DB" + DBAdaptType.class.getSimpleName()
						+ "(type) must be a concrete type"
						+ ", on " + javaProperty);
			}
		}

		// validation: Type adaptor's external type must be either:
		//   a) castable to the external property type (and not a QDT), or
		//   b) a simple type that is supported by the external property type,
		//      and the external property type must be a QDT
		// (note: in either case can't be a QDT itself due to rules above)
		if (typeAdaptorExternalType != null && !QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
			if (!javaProperty.type().equals(typeAdaptorExternalType)
					&& SafeOneWaySimpleTypeAdaptor.getSimpleCastFor(javaProperty.type(), typeAdaptorExternalType) == null) {
				throw new InvalidDeclaredTypeException("Type adaptor's external " + typeAdaptorExternalType.getSimpleName()
						+ " type is not compatible with the property type, on " + javaProperty);
			}
		}
		if (typeAdaptorExternalType != null && QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
			Class<? extends QueryableDatatype<?>> explicitQDTType = (Class<? extends QueryableDatatype<?>>) javaProperty.type();
			Class<?> inferredQDTType = inferredQDTTypeForSimpleType(typeAdaptorExternalType);
			if (inferredQDTType == null) {
				throw new InvalidDeclaredTypeException("Type adaptor's external " + typeAdaptorExternalType.getSimpleName()
						+ " type is not a supported simple type, on " + javaProperty);
			} else if (!isSimpleTypeSupportedByQDT(typeAdaptorExternalType, explicitQDTType)) {
				throw new InvalidDeclaredTypeException("Type adaptor's external " + typeAdaptorExternalType.getSimpleName()
						+ " type is not compatible with a " + explicitQDTType.getSimpleName()
						+ " property, on " + javaProperty);
			}
		}

		// validation: Type adaptor's internal type must be either:
		//   a) a simple type that implies an internal QDT type,
		//      and no explicit QDT type is specified, or
		//   b) a simple type that is supported by the explicit internal QDT type,
		//      and the explicit internal QDT type is specified
		// (note: in either case can't be a QDT itself due to rule above)
		if (typeAdaptorInternalType != null && explicitTypeOrNullOf(dbAdaptTypeAnnotation) == null) {
			Class<?> inferredQDTType = inferredQDTTypeForSimpleType(typeAdaptorInternalType);
			if (inferredQDTType == null) {
				throw new InvalidDeclaredTypeException("Type adaptor's internal " + typeAdaptorInternalType.getSimpleName()
						+ " type is not a supported simple type, on " + javaProperty);
			}
		}
		if (typeAdaptorInternalType != null && explicitTypeOrNullOf(dbAdaptTypeAnnotation) != null) {
			Class<? extends QueryableDatatype<?>> explicitQDTType = explicitTypeOrNullOf(dbAdaptTypeAnnotation);
			Class<?> inferredQDTType = inferredQDTTypeForSimpleType(typeAdaptorInternalType);
			if (inferredQDTType == null) {
				throw new InvalidDeclaredTypeException("Type adaptor's internal " + typeAdaptorInternalType.getSimpleName()
						+ " type is not a supported simple type, on " + javaProperty);
			} else if (!isSimpleTypeSupportedByQDT(typeAdaptorInternalType, explicitQDTType)) {
				throw new InvalidDeclaredTypeException("Type adaptor's internal " + typeAdaptorInternalType.getSimpleName()
						+ " type is not compatible with " + explicitQDTType.getSimpleName()
						+ ", on " + javaProperty);
			}
		}

		// populate everything
		this.genericPropertyType = javaProperty.type();
		if (dbAdaptTypeAnnotation == null) {
			// populate when no annotation
			this.typeAdaptor = null;
			this.dbvPropertyType = (Class<? extends QueryableDatatype<?>>) javaProperty.type();
			this.internalQdtSyncer = null;
		} else if (identityOnly) {
			// populate identity-only information when type adaptor declared
			Class<? extends QueryableDatatype<?>> type = explicitTypeOrNullOf(dbAdaptTypeAnnotation);
			if (type == null && typeAdaptorInternalType != null) {
				type = inferredQDTTypeForSimpleType(typeAdaptorInternalType);
			}
			if (type == null) {
				throw new NullPointerException("null dbvPropertyType, this is an internal bug");
			}
			this.dbvPropertyType = type;

			this.typeAdaptor = null;
			this.internalQdtSyncer = null;
		} else {
			// initialise type adapting
			this.typeAdaptor = newTypeAdaptorInstanceGiven(javaProperty, dbAdaptTypeAnnotation);

			Class<? extends QueryableDatatype<?>> type = explicitTypeOrNullOf(dbAdaptTypeAnnotation);
			if (type == null && typeAdaptorInternalType != null) {
				type = inferredQDTTypeForSimpleType(typeAdaptorInternalType);
			}
			if (type == null) {
				throw new NullPointerException("null dbvPropertyType, this is an internal bug");
			}
			this.dbvPropertyType = type;

			Class<?> internalLiteralType = literalTypeOf(type);
			Class<?> externalLiteralType;
			if (QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
				externalLiteralType = literalTypeOf((Class<? extends QueryableDatatype<?>>) javaProperty.type());
			} else {
				externalLiteralType = javaProperty.type();
			}

			if (QueryableDatatype.class.isAssignableFrom(javaProperty.type())) {
				this.internalQdtSyncer = new QueryableDatatypeSyncer(javaProperty.qualifiedName(),
						this.dbvPropertyType, internalLiteralType, externalLiteralType, this.typeAdaptor);
			} else {
				this.internalQdtSyncer = new SimpleValueQueryableDatatypeSyncer(javaProperty.qualifiedName(),
						this.dbvPropertyType, internalLiteralType, externalLiteralType, this.typeAdaptor);
			}
		}
	}

	/**
	 * Infers the QDT-type that corresponds to the given simple type. Used to
	 * infer the QDT-type that should be used internally, based on the type
	 * supplied by the type adaptor.
	 *
	 * <p>
	 * Make sure to keep this in sync with {@link #literalTypeOf}.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a QDT the should work
	 */
	// FIXME: change to require exact matches, rather than 'instance of'
	private static Class<? extends QueryableDatatype<?>> inferredQDTTypeForSimpleType(Class<?> simpleType) {
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
		} else if (Boolean.class.isAssignableFrom(simpleType)) {
			return DBBoolean.class;
		}

		// all remaining types require explicit declaration
		return null;
	}

	/**
	 *
	 * <p>
	 * Make sure to keep this in sync with {@link #inferredQDTTypeForSimpleType}.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a standard Java class equivalent to the QDT
	 */
	private static Class<?> literalTypeOf(Class<? extends QueryableDatatype<?>> qdtType) {
		if (qdtType.equals(DBString.class)) {
			return String.class;
		} else if (qdtType.equals(DBStringTrimmed.class)) {
			return String.class;
		} else if (qdtType.equals(DBNumber.class)) {
			return Double.class;
		} else if (qdtType.equals(DBInteger.class)) {
			return Long.class;
		} else if (qdtType.equals(DBDate.class)) {
			return Date.class;
		} else if (qdtType.equals(DBBoolean.class)) {
			return Boolean.class;
		} else {
			throw new RuntimeException("Unrecognised QDT-type " + qdtType.getSimpleName());
		}
	}

	/**
	 * Tests whether the simpleType is supported by the given QDT-type. A simple
	 * type is supported by the QDT type iff the simple type implies a QDT-type,
	 * and:
	 * <ul>
	 * <li> the implied QDT-type is exactly the same as the given QDT-type, or
	 * <li> the implied QDT-type (eg: DBInteger) is instance-of assignable to the
	 * given QDT-type (eg: DBNumber), or
	 * <li> the implied QDT-type (eg: DBDate) is a super-class of the given given
	 * QDT-type (eg: DBSpecialDate).
	 * </ul>
	 *
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the simple type can be replaced by a QDT
	 */
	private static boolean isSimpleTypeSupportedByQDT(Class<?> simpleType,
			Class<? extends QueryableDatatype<?>> qdtType) {
		Class<?> inferredQDTType = inferredQDTTypeForSimpleType(simpleType);
		if (inferredQDTType != null) {
			if (qdtType.isAssignableFrom(inferredQDTType) || inferredQDTType.isAssignableFrom(qdtType)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Internal helper to support the way annotation attribute defaulting works.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return A QDT Class
	 */
	private static Class<? extends QueryableDatatype<?>> explicitTypeOrNullOf(DBAdaptType annotation) {
		if (annotation == null) {
			return null;
		}

		// detect default
		if (annotation.type().equals(DBUnknownDatatype.class)) {
			return null;
		}

		// return value
		return annotation.type();
	}

	/**
	 * Gets the DBv-centric type of the property, possibly after type adaption.
	 */
	public Class<? extends QueryableDatatype<?>> getQueryableDatatypeClass() {
		return dbvPropertyType;
	}

	/**
	 * Gets the type of the property, possibly after type adaption.
	 */
	public Class<?> getGenericClass() {
		return genericPropertyType;
	}

	/**
	 * Indicates whether the property's type is adapted by an explicit or implicit
	 * type adaptor. (Note: at present there is no support for implicit type
	 * adaptors)
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the property is type adapted
	 */
	public boolean isTypeAdapted() {
		return (dbAdaptTypeAnnotation != null);
	}

	/**
	 * Gets the DBv-centric value from the underlying java property, converting if
	 * needed. This method behaves correctly regardless of whether an
	 * {@link DBAdaptType} annotation is present.
	 *
	 * @param target object containing the property
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the DBv-centric property value
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 * @throws IllegalStateException if the underlying java property is not
	 * readable
	 */
	public QueryableDatatype<?> getJavaPropertyAsQueryableDatatype(Object target) {
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
			QueryableDatatype<?> externalQdt = (QueryableDatatype<?>) externalValue;

			// convert
			return internalQdtSyncer.setInternalQDTFromExternalQDT(externalQdt);
		} // get directly without type adaptor
		// (note: type checking was performed at creation time)
		else {
			return (QueryableDatatype) javaProperty.get(target);
		}
	}

	/**
	 * Sets the underlying java property according to the given DBv-centric value.
	 * This method behaves correctly regardless of whether an {@link DBAdaptType}
	 * annotation is present.
	 *
	 * @param target object containing the property
	 *
	 * @throws DBThrownByEndUserCodeException if any user code throws an exception
	 * @throws IllegalStateException if the underlying java property is not
	 * writable
	 */
	public void setJavaPropertyAsQueryableDatatype(Object target, QueryableDatatype<?> dbvValue) {
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
			QueryableDatatype<?> externalQdt = (QueryableDatatype<?>) externalValue;

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
	 * Constructs a new instance of the type adaptor referenced by the given
	 * annotation instance. Handles all exceptions and throws them as the
	 * appropriate runtime exceptions
	 *
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a DBTypeAdaptor
	 * @throws DBRuntimeException on unexpected internal errors, and
	 * @throws InvalidDeclaredTypeException on errors with the end-user supplied
	 * code
	 */
	private static DBTypeAdaptor<Object, Object> newTypeAdaptorInstanceGiven(JavaProperty property, DBAdaptType annotation) {
		Class<? extends DBTypeAdaptor<?, ?>> adaptorClass = annotation.value();
		if (adaptorClass == null) {
			// shouldn't be possible
			throw new DBRuntimeException("Encountered unexpected null " + DBAdaptType.class.getSimpleName()
					+ ".adptor() (probably a bug in DBvolution)");
		}

		if (adaptorClass.isInterface()) {
			throw new InvalidDeclaredTypeException("TypeAdaptor cannot be an interface (" + adaptorClass.getSimpleName()
					+ "), on property " + property.qualifiedName());
		}
		if (Modifier.isAbstract(adaptorClass.getModifiers())) {
			throw new InvalidDeclaredTypeException("TypeAdaptor cannot be an abstract class (" + adaptorClass.getSimpleName()
					+ "), on property " + property.qualifiedName());
		}

		try {
			adaptorClass.newInstance();
		} catch (InstantiationException e) {
			throw new InvalidDeclaredTypeException("Type adaptor " + adaptorClass.getName()
					+ " could not be constructed, on property "
					+ property.qualifiedName() + ": " + e.getMessage(), e);
		} catch (IllegalAccessException e) {
			throw new InvalidDeclaredTypeException("Type adaptor " + adaptorClass.getName()
					+ " could not be constructed, on property "
					+ property.qualifiedName() + ": " + e.getMessage(), e);
		}

		// get default constructor
		Constructor<? extends DBTypeAdaptor<?, ?>> constructor;
		try {
			constructor = adaptorClass.getConstructor();
		} catch (NoSuchMethodException e) {
			throw new InvalidDeclaredTypeException("Type adaptor " + adaptorClass.getName()
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
			throw new InvalidDeclaredTypeException(adaptorClass.getName() + " cannot be constructed (it is probably abstract), referenced by property "
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
			String msg = (cause.getLocalizedMessage() == null) ? "" : ": " + cause.getLocalizedMessage();
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
}
