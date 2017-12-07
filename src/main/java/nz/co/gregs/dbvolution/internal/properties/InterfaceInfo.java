package nz.co.gregs.dbvolution.internal.properties;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Used internally to extract information about generics in reflected classes.
 */
public class InterfaceInfo {

	private boolean interfaceImplementedByImplementation = false;
	private ParameterBounds[] typeArgumentBounds;

	/**
	 * Resolves the most concrete known type arguments against the given interface
	 * class, as specified an the implementation class, or one of its ancestors
	 * (supertype or interfaces). If the implementation class does not extend or
	 * implement the supertype orinterface (after a recursive search), this method
	 * returns {@code null}.
	 *
	 * @param interfaceClass the supertype class or interface you're looking for
	 * @param implementationObject the actual implementation object you're testing
	 * @throws UnsupportedOperationException if encounter generics that aren't
	 * handled yet
	 */
	public InterfaceInfo(Class<?> interfaceClass, Object implementationObject) {
		this(interfaceClass, implementationObject.getClass());
	}

	/**
	 * Resolves the most concrete known type arguments against the given interface
	 * class, as specified an the implementation class, or one of its ancestors
	 * (supertype or interfaces). If the implementation class does not extend or
	 * implement the supertype orinterface (after a recursive search), this method
	 * returns {@code null}.
	 *
	 * @param interfaceClass the supertype class or interface you're looking for
	 * @param implementationClass the actual implementation class you're testing
	 * @throws UnsupportedOperationException if encounter generics that aren't
	 * handled yet
	 */
	public InterfaceInfo(Class<?> interfaceClass, Class<?> implementationClass) {
		this.typeArgumentBounds = getParameterBounds(interfaceClass, implementationClass);
		this.interfaceImplementedByImplementation = (typeArgumentBounds != null);
	}

	/**
	 * Indicates whether the implementation type actually makes any attempt to
	 * implement the interface type.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE or FALSE
	 */
	public boolean isInterfaceImplementedByImplementation() {
		return interfaceImplementedByImplementation;
	}

	/**
	 * Gets the concrete parameter values as bounds. The value bounds are resolved
	 * to the most concrete known values against the given interface class, as
	 * specified an the implementation class, or one of its ancestors (supertype
	 * or interfaces).
	 *
	 * <p>
	 * Then method returns {@code null} if the implementation class does not
	 * extend or implement the supertype or interface (recursively).
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the parameter values, ordered according to the parameters on the
	 * configured interface class; empty array if the implementation class
	 * implements or extends the interface or supertype, but that the
	 * interface/supertype has no generic parameters; null if the implementation
	 * class does not implement or extend the interface or supertype.
	 */
	public ParameterBounds[] getInterfaceParameterValueBounds() {
		if (typeArgumentBounds == null) {
			return null;
		} else {
			return Arrays.copyOf(typeArgumentBounds, typeArgumentBounds.length);
		}
	}

	/**
	 * Convenience method for getting default parameter bounds for each parameter
	 * of the given class.
	 *
	 * @param clazz the class to inspect
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the list of known and understood bounds for each parameter; empty
	 * bounds if the interface has no parameters; null if doesn't extend or
	 * implement the interface class (directly or indirectly)
	 * @throws UnsupportedOperationException if any generic type references are
	 * encountered that are not understood
	 */
	protected static ParameterBounds[] getParameterBounds(Class<?> clazz) {
		return getParameterBounds(clazz, clazz);
	}

	/**
	 * Resolves the most concrete known type arguments against the given interface
	 * class, as specified an the implementation class, or one of its ancestors
	 * (supertype or interfaces). If the implementation class does not implement
	 * the interface (after a recursive search), {@code null} is returned.
	 *
	 * @param interfaceClass the supertype class or interface you're looking for
	 * @param implementationClass the actual implementation class you're testing
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the list of known and understood bounds for each parameter; empty
	 * bounds if the interface has no parameters; null if doesn't extend or
	 * implement the interface class (directly or indirectly)
	 * @throws UnsupportedOperationException if any generic type references are
	 * encountered that are not understood
	 */
	protected static ParameterBounds[] getParameterBounds(Class<?> interfaceClass, Class<?> implementationClass) {
		return getParameterBounds(interfaceClass, implementationClass, null);
	}

	/**
	 * Resolves the most concrete known type arguments against the given interface
	 * class, as specified an the implementation class, or one of its ancestors
	 * (supertype or interfaces). If the implementation class does not implement
	 * the interface (after a recursive search), {@code null} is returned.
	 *
	 * @param interfaceClass interfaceClass
	 * @param implementationClass implementationClass
	 * @param argumentValues bound type argument values for each of the type
	 * arguments on the implementation class (empty array if none, null if not yet
	 * defined)
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return null if doesn't implement the interface, empty bounds if the
	 * interface has no parameters, or the list of known and understood bounds for
	 * each parameter
	 * @throws UnsupportedOperationException if any generic type references are
	 * encountered that are not understood
	 */
	protected static ParameterBounds[] getParameterBounds(Class<?> interfaceClass, Class<?> implementationClass,
			ParameterBounds[] argumentValues) {
		// parse argument values against type variables of implementation class
		Map<String, ParameterBounds> argumentValueByTypeVariableName = null;
		if (argumentValues != null) {
			TypeVariable<? extends Class<?>>[] typeVariables = implementationClass.getTypeParameters();

			// sanity check
			if (typeVariables.length != argumentValues.length) {
				// unexpected exception, this shouldn't be possible
				throw new UnsupportedOperationException("Encountered mismatched number of type parameters ("
						+ typeVariables.length + ") and values ("
						+ argumentValues.length + ") on " + implementationClass.getSimpleName()
						+ ": values=" + Arrays.toString(argumentValues));
			}

			argumentValueByTypeVariableName = new HashMap<String, ParameterBounds>();
			for (int i = 0; i < argumentValues.length && i < typeVariables.length; i++) {
				argumentValueByTypeVariableName.put(typeVariables[i].getName(), argumentValues[i]);
			}
		}

		// check if sitting on target
		if (implementationClass.equals(interfaceClass)) {
			return ParameterBounds.boundsForParametersOf(implementationClass, argumentValueByTypeVariableName);
		}

		// get bounds from "implements InterfaceClass"
		// (assume either ParameterizedType or Class)
		Type implementedInterfaceType = ancestorTypeByClass(interfaceClass, implementationClass);
		if (implementedInterfaceType != null) {
			return ParameterBounds.boundsForParametersOf(implementedInterfaceType, argumentValueByTypeVariableName);
		}

		// retrieve bounds from supertype and interface references
		for (Type ancestorType : ancestorTypesOf(implementationClass)) {
			ParameterBounds[] ancestorArgumentValues = ParameterBounds.boundsForParametersOf(ancestorType, argumentValueByTypeVariableName);

			// recurse into type
			try {
				Class<?> ancestorClass = resolveClassOf(ancestorType);
				ParameterBounds[] result = getParameterBounds(interfaceClass, ancestorClass, ancestorArgumentValues);
				if (result != null) {
					return result;
				}
			} catch (UnsupportedType dropped) {
				// try next ancestor
			}
		}

		// doesn't implement the interface
		return null;
	}

	/**
	 * Gets all direct ancestors of the given class, including its supertype and
	 * all directly implemented interfaces. Excludes ancestors of type
	 * {@code Object}.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return non-null list, empty if only ancestor is {@code Object}
	 */
	private static List<Type> ancestorTypesOf(Class<?> child) {
		List<Type> ancestors = new ArrayList<Type>();

		// get "extends xxx"
		Type supertype = child.getGenericSuperclass();
		if (supertype != null && !Object.class.equals(supertype)) {
			ancestors.add(supertype);
		}

		// get all "implements yyy"
		Type[] interfaces = child.getGenericInterfaces();
		if (interfaces != null) {
			ancestors.addAll(Arrays.asList(interfaces));
		}

		return ancestors;
	}

	/**
	 * Non-recursive: checks on this class only.
	 *
	 * @param ancestorClass a supertype or interface
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the actual Type reference to the interface class or null if not
	 * implemented/extended
	 */
	private static Type ancestorTypeByClass(Class<?> ancestorClass, Class<?> child) {
		// look for "extends InterfaceClass"
		Type supertype = child.getGenericSuperclass();
		if (supertype != null && !supertype.equals(Object.class)) {
			try {
				Class<?> supertypeClass = resolveClassOf(supertype);
				if (supertypeClass.equals(ancestorClass)) {
					return supertype;
				}
			} catch (UnsupportedType dropped) {
				// try next
			}
		}

		// look for "implements InterfaceClass"
		Type[] implementedInterfaces = child.getGenericInterfaces();
		for (Type implementedInterface : implementedInterfaces) {
			try {
				Class<?> implementedInterfaceClass = resolveClassOf(implementedInterface);
				if (implementedInterfaceClass.equals(ancestorClass)) {
					return implementedInterface;
				}
			} catch (UnsupportedType dropped) {
				// try next
			}
		}

		return null;
	}

	// supports only classes and parameterized types
	// doesn't support Array[] types or wildcard types
	private static Class<?> resolveClassOf(Type type) throws UnsupportedType {
		if (type instanceof Class<?>) {
			return (Class<?>) type;
		} else if (type instanceof GenericArrayType) {
			return resolveClassOf(((GenericArrayType) type).getGenericComponentType());
		} else if (type instanceof ParameterizedType) {
			return resolveClassOf(((ParameterizedType) type).getRawType());
		} else {
			throw new UnsupportedType("Can't yet handle " + type.getClass().getSimpleName() + " types");
		}
	}

	/**
	 * Converts the given type into a concise representation suitable for
	 * inclusion in error messages and logging.
	 *
	 * @param type	type
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a String describing the type succinctly
	 */
	protected static String descriptionOf(Type type) {
		try {
			return descriptionOf(type, new HashSet<TypeVariable<?>>());
		} catch (RuntimeException dropped) {
			// drop exception and quietly handle because this method is called in the context of other error handling
			// and musn't cause its own errors
			return type.toString();
		}
	}

	/**
	 * Converts the given type array into a concise representation suitable for
	 * inclusion in error messages and logging.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a String describing the types succinctly.
	 */
	static String descriptionOf(Type[] types) {
		try {
			return descriptionOf(types, new HashSet<TypeVariable<?>>());
		} catch (RuntimeException dropped) {
			// drop exception and quietly handle because this method is called in the context of other error handling
			// and musn't cause its own errors
			return Arrays.toString(types);
		}
	}

	// returns the description or empty string of types array is empty
	private static String descriptionOf(Type[] types, Set<TypeVariable<?>> observedTypeVariables) {
		return conditionalDescriptionOf(null, types, null, observedTypeVariables);
	}

	// returns the description or empty string of types array is empty
	private static String conditionalDescriptionOf(String conditionalPrefix, Type[] types, String conditionalPostfix,
			Set<TypeVariable<?>> observedTypeVariables) {
		StringBuilder buf = new StringBuilder();
		if (types != null && types.length > 0) {
			boolean first = true;
			if (conditionalPrefix != null) {
				buf.append(conditionalPrefix);
			}

			for (Type type : types) {
				if (!first) {
					buf.append(",");
				}
				buf.append(descriptionOf(type, observedTypeVariables));
				first = false;
			}

			if (conditionalPostfix != null) {
				buf.append(conditionalPostfix);
			}
		}
		return buf.toString();
	}

	private static String descriptionOf(Type type, Set<TypeVariable<?>> observedTypeVariables) {
		StringBuilder buf = new StringBuilder();

		// handle nulls
		if (type == null) {
			buf.append("null");
		} // handle simple class references
		else if (type instanceof Class) {
			buf.append(((Class<?>) type).getSimpleName());
		} // handle parameterized type references, eg: "MyInterface<T,Q extends QueryableDatatype>"
		else if (type instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) type;
			buf.append(descriptionOf(pt.getRawType()));
			buf.append(conditionalDescriptionOf("<", pt.getActualTypeArguments(), ">", observedTypeVariables));
		} // handle type variables, eg: "T extends Number"
		else if (type instanceof TypeVariable) {
			// format name and generic declaration together as: "MyInterface.T"
			TypeVariable<? extends GenericDeclaration> typeVariable = (TypeVariable<? extends GenericDeclaration>) type;
			buf.append(typeVariable.getName());

			// format bounds: " extends BoundType"
			if (!observedTypeVariables.contains(typeVariable)) {
				if (typeVariable.getBounds() != null && typeVariable.getBounds().length > 0) {
					buf.append(" extends ");

					Set<TypeVariable<?>> nestedObservedTypeVariables = new HashSet<TypeVariable<?>>(observedTypeVariables);
					nestedObservedTypeVariables.add(typeVariable);

					boolean first = true;
					for (Type boundingType : typeVariable.getBounds()) {
						if (!first) {
							buf.append(",");
						}
						String boundingTypeDescr = descriptionOf(boundingType, nestedObservedTypeVariables);
						if (boundingTypeDescr.contains(" ")) {
							buf.append("(").append(boundingTypeDescr).append(")");
						} else {
							buf.append(boundingTypeDescr);
						}
						first = false;
					}
				}
			}
		} // handle wildcards, eg "? extends MyInterface" and "? super Number"
		else if (type instanceof WildcardType) {
			WildcardType wildcard = (WildcardType) type;

			buf.append("?");
			if (wildcard.getUpperBounds() != null && wildcard.getUpperBounds().length > 0) {
				buf.append(" extends ");
				boolean first = true;
				for (Type boundingType : wildcard.getUpperBounds()) {
					if (!first) {
						buf.append(",");
					}
					String boundingTypeDescr = descriptionOf(boundingType, observedTypeVariables);
					if (boundingTypeDescr.contains(" ")) {
						buf.append("(").append(boundingTypeDescr).append(")");
					} else {
						buf.append(boundingTypeDescr);
					}
					first = false;
				}
			}
			if (wildcard.getLowerBounds() != null && wildcard.getLowerBounds().length > 0) {
				buf.append(" super ");
				boolean first = true;
				for (Type boundingType : wildcard.getLowerBounds()) {
					if (!first) {
						buf.append(",");
					}
					String boundingTypeDescr = descriptionOf(boundingType, observedTypeVariables);
					if (boundingTypeDescr.contains(" ")) {
						buf.append("(").append(boundingTypeDescr).append(")");
					} else {
						buf.append(boundingTypeDescr);
					}
					first = false;
				}
			}
		} // handle generic arrays, eg: "T[]" and "List<String>[]"
		else if (type instanceof GenericArrayType) {
			GenericArrayType array = (GenericArrayType) type;

			String typeDescr = descriptionOf(array.getGenericComponentType(), observedTypeVariables);
			if (typeDescr.contains(" ")) {
				buf.append("(").append(typeDescr).append(")");
			} else {
				buf.append(typeDescr);
			}
			buf.append("[]");
		} // handle unexpected cases
		else {
			buf.append(type.toString());
		}

		return buf.toString();
	}

	/**
	 * Represents the known and understood bounds of a type variable. If not
	 * understood, references of this type should be null. The widest open bound
	 * looks like an upper bound of {@code Object} and a null lower bound.
	 *
	 * <p>
	 * The bounds types themselves can be one of the following supported types:
	 * <ul>
	 * <li> Class
	 * <li> GenericArrayType
	 * </ul>
	 * All other {@code Type}s are either expanded out into the types above, or
	 * they are not supported. ParameterizedType is the only type that is not
	 * supported. Attempts to use it will result in an
	 * UnsupportedOperationException.
	 */
	public static class ParameterBounds {

		private final Type[] upperTypes; // always null or non-empty
		private final Type[] lowerTypes; // always null or non-empty

		/**
		 * Gets a default single bound given no further information. This has an
		 * upper bound of {@code Object}, and no lower bound.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return a ParameterBounds
		 */
		public static ParameterBounds defaultBounds() {
			return new ParameterBounds(new Type[]{Object.class}, null);
		}

		/**
		 * Gets parameter bounds for each of the generic type arguments of the given
		 * class or parameterized type reference. If the given class has bounded
		 * type parameters, the returned bounds will reflect that.
		 *
		 * <p>
		 * The {@code specifiedValuesByTypeVariableName} map is used for populating
		 * referenced values where type variable references are used. This is
		 * limited to the use of parameterized type references, and to only those
		 * type arguments which refer by type variable (ie: excludes direct class
		 * name references and other type references).
		 *
		 * <p>
		 * If the class has no generic parameters, an empty array is returned.
		 *
		 * @param parameterizedTypeRef a Class or ParameterizedType
		 * @param paramValuesByTypeVariableName a map from TypeVariable name to
		 * actual specified bounds; must contain values for all type variable
		 * references; null if none defined
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 * @return an array of ParameterBounds object.
		 * @throws UnsupportedOperationException if not a class or parameterized
		 * type
		 */
		public static ParameterBounds[] boundsForParametersOf(Type parameterizedTypeRef,
				Map<String, ParameterBounds> paramValuesByTypeVariableName) {
			// Class reference without parameters: use default bound for each parameter
			// in reference interface type (this is correct handling for jdk1.4 style code)
			if (parameterizedTypeRef instanceof Class) {
				return boundsForParametersOf((Class<?>) parameterizedTypeRef);
			} // Parameterized class reference: use bounds as they are provided
			else if (parameterizedTypeRef instanceof ParameterizedType) {
				return boundsForParametersOf((ParameterizedType) parameterizedTypeRef,
						paramValuesByTypeVariableName);
			} // refuse to process other types
			// (not actually expecting any other types anyway)
			else {
				throw new UnsupportedOperationException(
						"Expecting only Class and ParameterizedType references, encountered "
						+ parameterizedTypeRef.getClass().getSimpleName() + ": " + descriptionOf(parameterizedTypeRef));
			}
		}

		/**
		 * Gets default parameter bounds for each of the generic parameters of the
		 * given class. If the given class has bounded type parameters, the returned
		 * bounds will reflect that. If the class has no generic parameters, an
		 * empty array is returned.
		 *
		 * @param parameterizedClass	parameterizedClass
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 * @return an array of ParameterBounds objects
		 */
		public static ParameterBounds[] boundsForParametersOf(Class<?> parameterizedClass) {
			List<ParameterBounds> bounds = new ArrayList<ParameterBounds>();
			for (TypeVariable<? extends Class<?>> typeVariable : parameterizedClass.getTypeParameters()) {
				bounds.add(getBoundsOf(typeVariable));
			}
			return bounds.toArray(new ParameterBounds[]{});
		}

		/**
		 * Gets parameter bounds for each of the actual type arguments in the given
		 * parameterized class reference. If the type arguments are bounded, the
		 * returned bounds instances will reflect that.
		 *
		 * <p>
		 * The {@code specifiedValuesByTypeVariableName} map is used for populating
		 * referenced values where type variable references are used. This is
		 * limited to the use of parameterized type references, and to only those
		 * type arguments which refer by type variable (ie: excludes direct class
		 * name references and other type references).
		 *
		 * @param parameterizedType	parameterizedType
		 * @param paramValuesByTypeVariableName a map from TypeVariable name to
		 * actual specified bounds; must contain values for all type variable
		 * references
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 * @return an array of bounds, one item for each type argument
		 */
		public static ParameterBounds[] boundsForParametersOf(ParameterizedType parameterizedType,
				Map<String, ParameterBounds> paramValuesByTypeVariableName) {
			List<ParameterBounds> allBounds = new ArrayList<ParameterBounds>();

			for (Type typeArgument : parameterizedType.getActualTypeArguments()) {
				// use pre-defined values
				if (paramValuesByTypeVariableName != null && typeArgument instanceof TypeVariable) {
					TypeVariable<?> typeVariable = (TypeVariable<?>) typeArgument;
					ParameterBounds value = paramValuesByTypeVariableName.get(typeVariable.getName());
					if (value == null) {
						throw new UnsupportedOperationException("No known value for TypeVariable " + typeVariable.getName() + " "
								+ "in " + paramValuesByTypeVariableName + " "
								+ "when extracting parameters of " + descriptionOf(parameterizedType));
					}
					allBounds.add(value);
				} // derive from definitions
				else {
					allBounds.add(getBoundsOf(typeArgument));
				}
			}
			return allBounds.toArray(new ParameterBounds[]{});
		}

		/**
		 * Creates a single bounds instance from the supplied type reference.
		 *
		 * @param type	type
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 * @return the single bounds instance created
		 */
		public static ParameterBounds getBoundsOf(Type type) {
			if (type instanceof Class<?>) {
				return new ParameterBounds(new Type[]{type}, null);
			} else if (type instanceof GenericArrayType) {
				return new ParameterBounds(new Type[]{type}, null);
			} else if (type instanceof TypeVariable) {
				TypeVariable<?> typeVariable = (TypeVariable<?>) type;
				return new ParameterBounds(typeVariable.getBounds(), null);
			} else if (type instanceof WildcardType) {
				WildcardType wildcard = (WildcardType) type;
				return new ParameterBounds(wildcard.getUpperBounds(), wildcard.getLowerBounds());
			} else if (type instanceof ParameterizedType) {
				// experimental support for parameterized type references
				// (makes no attempt to understand what's inside it,
				//  included so caller can return detailed error)
				return new ParameterBounds(new Type[]{type}, null);
			} else {
				throw new UnsupportedOperationException("Unsupported type " + type.getClass().getSimpleName() + ": " + descriptionOf(type));
			}
		}

		/**
		 * Creates a new instance with the given bounds. Only supported types may be
		 * supplied.
		 *
		 * @param upperTypes nulls and empty arrays are converted to
		 * {@code [Object]}.
		 * @param lowerTypes empty arrays are converted to null
		 */
		public ParameterBounds(Type[] upperTypes, Type[] lowerTypes) {
			this.upperTypes = (upperTypes == null || upperTypes.length == 0) ? new Type[]{Object.class} : upperTypes;
			this.lowerTypes = (lowerTypes == null || lowerTypes.length == 0) ? null : lowerTypes;

		}

		/**
		 * Gets a string representation suitable for debugging.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return a string of this object.
		 */
		@Override
		public String toString() {
			StringBuilder buf = new StringBuilder();
			if (isUpperMulti()) {
				buf.append("{").append(descriptionOf(upperTypes)).append("}");
			} else {
				buf.append(descriptionOf(upperTypes));
			}

			if (lowerTypes != null) {
				buf.append(" super ");
				if (isUpperMulti()) {
					buf.append("{").append(descriptionOf(lowerTypes)).append("}");
				} else {
					buf.append(descriptionOf(lowerTypes));
				}
			}
			return buf.toString();
		}

		boolean hasUpperBound() {
			return (upperTypes != null);
		}

		boolean hasLowerBound() {
			return (lowerTypes != null);
		}

		boolean isUpperMulti() {
			return (upperTypes != null) && (upperTypes.length > 1);
		}

		boolean isLowerMulti() {
			return (lowerTypes != null) && (lowerTypes.length > 1);
		}

		/**
		 * Assumes there's only one upper class and returns it.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the non-null upper class (defaults to {@code Object})
		 * @throws UnsupportedType if type reference cannot be converted to a class
		 * @throws IllegalStateException if there's actually more than one class
		 */
		Class<?> upperClass() throws UnsupportedType {
			Type type = upperType();
			if (type != null) {
				return resolveClassOf(type);
			}
			return Object.class;
		}

		/**
		 * Assumes there's only one lower class and returns it.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the lower class or null if none
		 * @throws UnsupportedType if type reference cannot be converted to a class
		 * @throws IllegalStateException if there's actually more than one class
		 */
		public Class<?> lowerClass() throws UnsupportedType {
			Type type = lowerType();
			if (type != null) {
				return resolveClassOf(type);
			}
			return null;
		}

		/**
		 * Gets the upper bounding classes. This method returns the equivalent of
		 * {@link #upperTypes()} after resolving types to classes.
		 *
		 * <p>
		 * In most cases only one type will be supplied. Multiple are used where a
		 * generic type reference is of form {@code T extends TypeOne,TypeTwo}. For
		 * example, it can be used to require that a type <i>both</i> is an enum,
		 * and implements an particular interface.
		 *
		 * <p>
		 * If the upper bound has not been specialised, it will be
		 * {@code Object.class}.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return non-empty upper bounding types (usually only one)
		 * @throws UnsupportedType if type reference cannot be converted to a class
		 */
		public Class<?>[] upperClasses() throws UnsupportedType {
			if (upperTypes == null) {
				return null;
			}
			Class<?>[] classes = new Class<?>[upperTypes.length];
			for (int i = 0; i < classes.length; i++) {
				classes[i] = resolveClassOf(upperTypes[i]);
			}
			return classes;
		}

		/**
		 * Gets the lower bounding classes. This method returns the equivalent of
		 * {@link #upperTypes()} after resolving types to classes.
		 *
		 * <p>
		 * In most cases only one type will be supplied. Multiple are used where a
		 * generic type reference is of form {@code T super TypeOne,TypeTwo}.
		 *
		 * <p>
		 * If the lower bound has not been specialised, it will be null.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return null if no lower bound, or non-empty bounding types (usually only
		 * one)
		 * @throws UnsupportedType if type reference cannot be converted to a class
		 */
		public Class<?>[] lowerClasses() throws UnsupportedType {
			if (lowerTypes == null) {
				return null;
			}
			Class<?>[] classes = new Class<?>[lowerTypes.length];
			for (int i = 0; i < classes.length; i++) {
				classes[i] = resolveClassOf(lowerTypes[i]);
			}
			return classes;
		}

		/**
		 * Assumes there's only one upper type and returns it.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the non-null upper type (defaults to {@code Object})
		 * @throws IllegalStateException if there's actually more than one type
		 */
		public Type upperType() {
			if (upperTypes != null && upperTypes.length > 1) {
				throw new IllegalStateException("Cannot get single type where multiple types are used");
			}
			return (upperTypes == null) ? null : upperTypes[0];
		}

		/**
		 * Assumes there's only one lower type and returns it.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return the lower type or null if none
		 * @throws IllegalStateException if there's actually more than one type
		 */
		public Type lowerType() {
			if (lowerTypes != null && lowerTypes.length > 1) {
				throw new IllegalStateException("Cannot get single type where multiple types are used");
			}
			return (lowerTypes == null) ? null : lowerTypes[0];
		}

		/**
		 * Gets the upper bounding types.
		 *
		 * <p>
		 * In most cases only one type will be supplied. Multiple are used where a
		 * generic type reference is of form {@code T extends TypeOne,TypeTwo}. For
		 * example, it can be used to require that a type <i>both</i> is an enum,
		 * and implements an particular interface.
		 *
		 * <p>
		 * If the upper bound has not been specialised, it will be
		 * {@code Object.class}.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return non-empty upper bounding types (usually only one)
		 */
		public Type[] upperTypes() {
			if (upperTypes == null) {
				return null;
			} else {
				return Arrays.copyOf(upperTypes, upperTypes.length);
			}
		}

		/**
		 * Gets the lower bounding types.
		 *
		 * <p>
		 * In most cases only one type will be supplied. Multiple are used where a
		 * generic type reference is of form {@code T super TypeOne,TypeTwo}.
		 *
		 * <p>
		 * If the lower bound has not been specialised, it will be null.
		 *
		 * <p style="color: #F90;">Support DBvolution at
		 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
		 *
		 * @return null if no lower bound, or non-empty bounding types (usually only
		 * one)
		 */
		public Type[] lowerTypes() {
			if (lowerTypes == null) {
				return null;
			} else {
				return Arrays.copyOf(lowerTypes, lowerTypes.length);
			}
		}
	}

	/**
	 * Thrown internally when a Type is not supported by a method
	 */
	static class UnsupportedType extends Exception {

		private static final long serialVersionUID = 1L;

		UnsupportedType(String message) {
			super(message);
		}
	}
}
