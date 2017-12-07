package nz.co.gregs.dbvolution.internal.properties;

import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;

public class TypeTestUtils {

	public static void describeClass(Class<?> classToDescribe) {
		System.out.println("Methods of this=" + classToDescribe.getSimpleName() + ":");
		System.out.println("  (class: synthetic=" + classToDescribe.isSynthetic() + ","
				+ " interface=" + classToDescribe.isInterface() + ","
				+ " abstract=" + (Modifier.isAbstract(classToDescribe.getModifiers()))
				+ ")");

		TypeVariable<? extends Class<?>>[] typeVariables = classToDescribe.getTypeParameters();
		if (typeVariables != null && typeVariables.length > 0) {

			System.out.println("  (class: typeVariables=<" + descriptionOf(typeVariables, classToDescribe) + ">)");
		}

		if (classToDescribe.getSuperclass() != null) {
			System.out.print("  (class: supertype=");
			System.out.print(descriptionOf(classToDescribe.getGenericSuperclass(), classToDescribe));
			System.out.println(")");
		}

		Type[] genericInterfaces = classToDescribe.getGenericInterfaces();
		if (genericInterfaces != null && genericInterfaces.length > 0) {
			System.out.print("  (class: implements=");
			System.out.print(descriptionOf(genericInterfaces, classToDescribe));
			System.out.println(")");
		}

		// note: skip over methods defined on Object
		for (Method method : classToDescribe.getMethods()) {
			if (!method.getDeclaringClass().equals(Object.class)) {
				System.out.println("  " + descriptionOf(method, classToDescribe));
			}
		}
		System.out.println();
	}

	public static String descriptionOf(Method method) {
		return descriptionOf(method, null);
	}

	private static String descriptionOf(Method method, GenericDeclaration context) {
		StringBuilder buf = new StringBuilder();
		//buf.append(" ");

		if (context != null && context.equals(method.getDeclaringClass())) {
			buf.append("this.");
		} else {
			buf.append(method.getDeclaringClass().getSimpleName());
			buf.append(".");
		}
		buf.append(method.getName());

		buf.append(conditionalDescriptionOf("<", method.getGenericParameterTypes(), ">", context));

		buf.append("(");
		boolean first = true;
		for (Class<?> paramType : method.getParameterTypes()) {
			if (!first) {
				buf.append(",");
			}
			buf.append(paramType.getSimpleName());
			first = false;
		}
		if (method.isVarArgs()) {
			buf.append("..."); // applies to last parameter
		}
		buf.append(")");

		buf.append(": ");

		buf.append(method.getGenericReturnType() == null ? "void" : descriptionOf(method.getGenericReturnType(), context));

		if (method.isSynthetic() || method.isBridge() || Modifier.isAbstract(method.getModifiers())) {
			buf.append("    {");
			first = true;
			if (Modifier.isAbstract(method.getModifiers())) {
				if (!first) {
					buf.append(",");
				}
				buf.append("abstract");
				first = false;
			}
			if (method.isSynthetic()) {
				if (!first) {
					buf.append(",");
				}
				buf.append("synthetic");
				first = false;
			}
			if (method.isBridge()) {
				if (!first) {
					buf.append(",");
				}
				buf.append("bridge");
				first = false;
			}
			buf.append("}");
		}

		return buf.toString();
	}

	// returns the description or empty string of types array is empty
	public static String descriptionOf(Type[] types) {
		return conditionalDescriptionOf(null, types, null);
	}

	public static String descriptionOf(Type[] types, GenericDeclaration context) {
		return conditionalDescriptionOf(null, types, null, context);
	}

	// returns the description or empty string of types array is empty
	public static String conditionalDescriptionOf(Type[] types) {
		return conditionalDescriptionOf(null, types, null);
	}

	public static String conditionalDescriptionOf(Type[] types, GenericDeclaration context) {
		return conditionalDescriptionOf(null, types, null, context);
	}

	// returns the description or empty string of types array is empty
	public static String conditionalDescriptionOf(String conditionalPrefix, Type[] types, String conditionalPostfix) {
		return conditionalDescriptionOf(conditionalPrefix, types, conditionalPostfix, null);
	}

	// returns the description or empty string of types array is empty
	public static String conditionalDescriptionOf(String conditionalPrefix, Type[] types, String conditionalPostfix, GenericDeclaration context) {
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
				buf.append(descriptionOf(type, context));
				first = false;
			}

			if (conditionalPostfix != null) {
				buf.append(conditionalPostfix);
			}
		}
		return buf.toString();
	}

	/**
	 * Converts the given type into a concise representation.
	 *
	 * @param type
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a descriptive String
	 */
	public static String descriptionOf(Type type) {
		return descriptionOf(type, null);
	}

	@SuppressWarnings("unchecked")
	private static String descriptionOf(Type type, GenericDeclaration context) {
		StringBuilder buf = new StringBuilder();

		// handle nulls
		if (type == null) {
			buf.append("null");
		}

		// handle simple class references
		if (type instanceof Class) {
			buf.append(((Class<?>) type).getSimpleName());
		} // handle parameterized type references, eg: "MyInterface<T,Q extends QueryableDatatype>"
		else if (type instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) type;

			if (pt.getOwnerType() != null && context != null && context.equals(pt.getOwnerType())) {
				buf.append("this.");
			} else if (pt.getOwnerType() != null) {
				String ownerTypeDesc = descriptionOf(pt.getOwnerType());
				if (ownerTypeDesc.contains(" ")) {
					buf.append("(").append(ownerTypeDesc).append(")");
				} else {
					buf.append(ownerTypeDesc);
				}
				buf.append(".");
			}

			buf.append(descriptionOf(pt.getRawType(), context));

			buf.append(conditionalDescriptionOf("<", pt.getActualTypeArguments(), ">", context));
		} // handle type variables, eg: "T extends Number"
		else if (type instanceof TypeVariable) {
			// format name and generic declaration together as: "MyInterface.T"
			TypeVariable<? extends GenericDeclaration> typeVariable = (TypeVariable<? extends GenericDeclaration>) type;
			String genericDeclarationName;
			if (typeVariable.getGenericDeclaration() == null) {
				genericDeclarationName = "null";
			} else if (typeVariable.getGenericDeclaration() instanceof Class) {
				genericDeclarationName = ((Class<?>) typeVariable.getGenericDeclaration()).getSimpleName();
				if (context != null && context.equals(typeVariable.getGenericDeclaration())) {
					genericDeclarationName = abbr(genericDeclarationName);
				}
			} else if (typeVariable.getGenericDeclaration() instanceof Constructor) {
				Constructor<?> c = (Constructor<?>) typeVariable.getGenericDeclaration();
				if (context != null && context.equals(c.getDeclaringClass())) {
					genericDeclarationName = abbr(c.getDeclaringClass().getSimpleName()) + "." + c.getName();
				} else {
					genericDeclarationName = c.getDeclaringClass().getSimpleName() + "." + c.getName();
				}
			} else if (typeVariable.getGenericDeclaration() instanceof Method) {
				Method m = (Method) typeVariable.getGenericDeclaration();
				if (context != null && context.equals(m.getDeclaringClass())) {
					genericDeclarationName = abbr(m.getDeclaringClass().getSimpleName()) + "." + m.getName() + "()";
				} else {
					genericDeclarationName = m.getDeclaringClass().getSimpleName() + "." + m.getName() + "()";
				}
			} else {
				throw new UnsupportedOperationException("Encountered GenericDeclaration I don't know how to handle ("
						+ typeVariable.getGenericDeclaration().getClass().getSimpleName() + "): "
						+ typeVariable.getGenericDeclaration());
			}
			buf.append(genericDeclarationName);
			buf.append(".");
			buf.append(typeVariable.getName());

			// format bounds: " extends BoundType"
			if (typeVariable.getBounds() != null && typeVariable.getBounds().length > 0) {
				buf.append(" extends ");
				boolean first = true;
				for (Type boundingType : typeVariable.getBounds()) {
					if (!first) {
						buf.append(",");
					}
					String boundingTypeDescr = descriptionOf(boundingType, context);
					if (boundingTypeDescr.contains(" ")) {
						buf.append("(").append(boundingTypeDescr).append(")");
					} else {
						buf.append(boundingTypeDescr);
					}
					first = false;
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
					String boundingTypeDescr = descriptionOf(boundingType, context);
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
					String boundingTypeDescr = descriptionOf(boundingType, context);
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

			String typeDescr = descriptionOf(array.getGenericComponentType(), context);
			if (typeDescr.contains(" ")) {
				buf.append("(").append(typeDescr).append(")");
			} else {
				buf.append(typeDescr);
			}
			buf.append("[]");
		} // handle unexpected cases
		else {
			throw new UnsupportedOperationException("Encountered Type I don't know how to handle ("
					+ type.getClass().getSimpleName() + "): " + type);
		}

		return buf.toString();
	}

	private static String abbr(String text) {
		//if (text.length() > 6) {
		//return text.substring(0,3)+"...";
		return "this";
		//}
		//return text;
	}

}
