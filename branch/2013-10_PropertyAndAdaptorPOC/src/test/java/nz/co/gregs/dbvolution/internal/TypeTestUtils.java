package nz.co.gregs.dbvolution.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;

import nz.co.gregs.dbvolution.internal.InterfaceImplementationUnderstandingTests.MyInterface;

import com.sun.jersey.core.reflection.ReflectionHelper;
import com.sun.jersey.core.reflection.ReflectionHelper.ClassTypePair;

public class TypeTestUtils {
	public static void describeClass(Class<?> classToDescribe) {
		System.out.println("Type adptor methods of "+classToDescribe.getSimpleName());
		System.out.println("  (class: synthetic="+classToDescribe.isSynthetic()+"," +
				" interface="+classToDescribe.isInterface()+","+
				" abstract="+(Modifier.isAbstract(classToDescribe.getModifiers()))+
				")");
		
		if (classToDescribe.getSuperclass() != null) {
			System.out.println("  (class: supertype="+classToDescribe.getSuperclass().getSimpleName()+")");
		}
		
		TypeVariable<? extends Class<?>>[] typeVariables = classToDescribe.getTypeParameters();
		if (typeVariables != null && typeVariables.length > 0) {
			System.out.print("  (class: typeVariables=<");
			boolean first = true;
			for (TypeVariable<? extends Class<?>> typeVariable: typeVariables) {
				if (!first) System.out.print(",");
				System.out.print(typeVariable);
				first = false;
			}
			System.out.print(">, ");

			System.out.print("types={"+conditionalDescriptionOf(typeVariables)+"}");
			System.out.println(")");
		}
		
		Type[] genericInterfaces = classToDescribe.getGenericInterfaces();
		if (genericInterfaces != null && genericInterfaces.length > 0) {
			System.out.print("  (class: implements=");
			boolean first = true;
			for (Type type: genericInterfaces) {
				if (!first) System.out.print(",");
				if (type instanceof ParameterizedType) {
					Class<?> clazz = (Class<?>)(((ParameterizedType) type).getRawType());
					System.out.print(clazz.getSimpleName());
					Type[] args = ((ParameterizedType) type).getActualTypeArguments();
					if (args != null && args.length > 0) {
						System.out.print("<");
						boolean first2 = true;
						for (Type arg: args) {
							if (!first2) System.out.print(",");
							if (arg instanceof TypeVariable) {
								System.out.print(((TypeVariable) arg).getName());
								System.out.print(":");
								System.out.print(((Class<?>)((TypeVariable) arg).getBounds()[0]).getSimpleName());
								//System.out.print(((TypeVariable) arg).getGenericDeclaration());
								
								
								ClassTypePair classTypePair = ReflectionHelper.resolveTypeVariable(classToDescribe, MyInterface.class, (TypeVariable) arg);
								if (classTypePair != null) {
									System.out.print("{"+classTypePair.c.getSimpleName()+"}");
								}
								
							}
							else if (arg instanceof Class) {
								System.out.print(((Class) arg).getSimpleName());
							}
							else {
								throw new UnsupportedOperationException("What is "+arg.getClass().getName());
							}
							first2 = false;
						}
						System.out.print(">");
					}
					
				}
				else if (type instanceof Class) {
					System.out.println(((Class<?>)type).getSimpleName());
				}
				else {
					throw new UnsupportedOperationException("What is "+type.getClass().getName());
				}
				//System.out.print(type);
				first = false;
			}
			System.out.println(")");
		}
		for (Method method: classToDescribe.getMethods()) {
			if (method.getName().equals("toObjectValue") || method.getName().equals("toDBvValue")) {
				System.out.println("  "+descriptionOf(method));
			}
		}
		System.out.println();
	}
	
	public static String descriptionOf(Method method) {
		StringBuilder buf = new StringBuilder();
		//buf.append(" ");
		
		buf.append(method.getDeclaringClass().getSimpleName());
		buf.append(".");
		buf.append(method.getName());
		
		buf.append(conditionalDescriptionOf("<",method.getGenericParameterTypes(),">"));
		
		buf.append("(");
		boolean first = true;
		for (Class<?> paramType: method.getParameterTypes()) {
			if (!first) buf.append(",");
			buf.append(paramType.getSimpleName());
			first = false;
		}
		if (method.isVarArgs()) {
			buf.append("..."); // applies to last parameter
		}
		buf.append(")");
		
		buf.append(": ");
		buf.append(method.getReturnType() == null ? "void" : method.getReturnType().getSimpleName());
		buf.append("/");
		buf.append(method.getGenericReturnType() == null ? "void" : descriptionOf(method.getGenericReturnType()));
		
		if (method.isSynthetic() || method.isBridge() || Modifier.isAbstract(method.getModifiers())) {
			buf.append("    {");
			first = true;
			if (Modifier.isAbstract(method.getModifiers())) {
				if (!first) buf.append(",");
				buf.append("abstract");
				first = false;
			}
			if (method.isSynthetic()) {
				if (!first) buf.append(",");
				buf.append("synthetic");
				first = false;
			}
			if (method.isBridge()) {
				if (!first) buf.append(",");
				buf.append("bridge");
				first = false;
			}
			buf.append("}");
		}
		
		return buf.toString();
	}

	// returns the description or empty string of types array is empty
	public static String conditionalDescriptionOf(Type[] types) {
		return conditionalDescriptionOf(null, types, null);
	}
	
	// returns the description or empty string of types array is empty
	public static String conditionalDescriptionOf(String conditionalPrefix, Type[] types, String conditionalPostfix) {
		StringBuilder buf = new StringBuilder();
		if (types != null && types.length > 0) {
			boolean first = true;
			if (conditionalPrefix != null) {
				buf.append(conditionalPrefix);
			}
			
			for (Type type: types) {
				if (!first) buf.append(",");
				buf.append(descriptionOf(type));
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
	 * @param type
	 * @return
	 */
	public static String descriptionOf(Type type) {
		StringBuilder buf = new StringBuilder();
		
		// handle nulls
		if (type == null) {
			buf.append("null");
		}
		
		// handle simple class references
		if (type instanceof Class) {
			buf.append(((Class<?>) type).getSimpleName());
		}
		
		// handle type variables, eg: "T extends Number"
		else if (type instanceof TypeVariable) {
			// format name and generic declaration together as: "MyInterface.T"
			TypeVariable<? extends GenericDeclaration> typeVariable = (TypeVariable<? extends GenericDeclaration>)type;
			String genericDeclarationName;
			if (typeVariable.getGenericDeclaration() == null) {
				genericDeclarationName = "null";
			}
			else if (typeVariable.getGenericDeclaration() instanceof Class) {
				genericDeclarationName = ((Class<?>) typeVariable.getGenericDeclaration()).getSimpleName();
			}
			else if (typeVariable.getGenericDeclaration() instanceof Constructor) {
				Constructor<?> c = (Constructor<?>)typeVariable.getGenericDeclaration();
				genericDeclarationName = c.getDeclaringClass().getSimpleName()+"."+c.getName();
			}
			else if (typeVariable.getGenericDeclaration() instanceof Method) {
				Method m = (Method)typeVariable.getGenericDeclaration();
				genericDeclarationName = m.getDeclaringClass().getSimpleName()+"."+m.getName()+"()";
			}
			else {
				throw new UnsupportedOperationException("Encountered GenericDeclaration I don't know how to handle ("+
						typeVariable.getGenericDeclaration().getClass().getSimpleName()+"): "+
						typeVariable.getGenericDeclaration());
			}
			buf.append(genericDeclarationName);
			buf.append(".");
			buf.append(typeVariable.getName());
			
			// format bounds: " extends BoundType"
			if (typeVariable.getBounds() != null && typeVariable.getBounds().length > 0) {
				buf.append(" extends ");
				boolean first = true;
				for (Type boundingType: typeVariable.getBounds()) {
					if (!first) buf.append(",");
					String boundingTypeDescr = descriptionOf(boundingType);
					if (boundingTypeDescr.contains(" ")) {
						buf.append("(").append(boundingTypeDescr).append(")");
					}
					else {
						buf.append(boundingTypeDescr);
					}
					first = false;
				}
			}
		}
		
		// handle wildcards, eg "? extends MyInterface" and "? super Number"
		else if (type instanceof WildcardType) {
			WildcardType wildcard = (WildcardType)type;
			
			buf.append("?");
			if (wildcard.getUpperBounds() != null && wildcard.getUpperBounds().length > 0) {
				buf.append(" extends ");
				boolean first = true;
				for (Type boundingType: wildcard.getUpperBounds()) {
					if (!first) buf.append(",");
					String boundingTypeDescr = descriptionOf(boundingType);
					if (boundingTypeDescr.contains(" ")) {
						buf.append("(").append(boundingTypeDescr).append(")");
					}
					else {
						buf.append(boundingTypeDescr);
					}
					first = false;
				}
			}
			if (wildcard.getLowerBounds() != null && wildcard.getLowerBounds().length > 0) {
				buf.append(" super ");
				boolean first = true;
				for (Type boundingType: wildcard.getLowerBounds()) {
					if (!first) buf.append(",");
					String boundingTypeDescr = descriptionOf(boundingType);
					if (boundingTypeDescr.contains(" ")) {
						buf.append("(").append(boundingTypeDescr).append(")");
					}
					else {
						buf.append(boundingTypeDescr);
					}
					first = false;
				}
			}
		}
		
		// handle generic arrays, eg: "T[]" and "List<String>[]"
		else if (type instanceof GenericArrayType) {
			GenericArrayType array = (GenericArrayType)type;
			
			String typeDescr = descriptionOf(array.getGenericComponentType());
			if (typeDescr.contains(" ")) {
				buf.append("(").append(typeDescr).append(")");
			}
			else {
				buf.append(typeDescr);
			}
			buf.append("[]");
		}
		
		// handle unexpected cases
		else {
			throw new UnsupportedOperationException("Encountered Type I don't know how to handle ("+
					type.getClass().getSimpleName()+"): "+type);
		}
		
		return buf.toString();
	}

}
