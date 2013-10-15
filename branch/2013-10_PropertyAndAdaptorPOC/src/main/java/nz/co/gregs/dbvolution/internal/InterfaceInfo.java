package nz.co.gregs.dbvolution.internal;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nz.co.gregs.dbvolution.DBRuntimeException;

public class InterfaceInfo {
	private Class<?> interfaceClass;
	private Class<?> implementationClass;
	private boolean interfaceImplementedByImplementation = false;
	
	/** Map from interface class method to implementation class method */
	private Map<Method, Method> implementationMethodsByInterfaceMethod = new HashMap<Method, Method>();
	
	
//	Type adptor methods of AbstractPartialImplementationWithWildcardType
//	  (class: synthetic=false, interface=false, abstract=true)
//	  (class: supertype=Object)
//	  (class: implements=DBTypeAdaptor<T:Number,Q:DBNumber>)
//	  Number toObjectValue(DBNumber)
//	  Object toObjectValue(QueryableDatatype)    {synthetic,bridge}
//	  QueryableDatatype toDBvValue(Object)

// Question: what do those things resolve to in, for example, Iterable<E>?
	
	
//	Methods of this=AbstractPartialImplementationWithConcreteType:
//		  (class: synthetic=false, interface=false, abstract=true)
//		  (class: supertype=Object)
//		  (class: implements=InterfaceImplementationUnderstandingTests.MyInterface<Number,DBNumber>)
// => use "implements MyInterface<T,Q>"
	
//	Methods of this=ConcretePartialImplementationOfConcreteType:
//		  (class: synthetic=false, interface=false, abstract=false)
//		  (class: supertype=AbstractPartialImplementationWithConcreteType)
// => have to recurse into supertype for "implements MyInterface<T,Q>"


//	Methods of this=AbstractPartialImplementationWithWildcardType:
//		  (class: synthetic=false, interface=false, abstract=true)
//		  (class: typeVariables=<this.T extends Number,this.Q extends DBNumber>)
//       note: could have been  <this.Q extends DBNumber,this.T extends Number> without breaking the 'implements' section.
//		  (class: supertype=Object)
//		  (class: implements=InterfaceImplementationUnderstandingTests.MyInterface<this.T extends Number,this.Q extends DBNumber>)
// => use "implements MyInterface<T,Q>"  --> {T extends Number, Q extends DBNumber}
// => then, because this is abstract, label those via the typeVariables section (via name "T" and "Q", and via order of typeVariables section)
		  
//	Methods of this=ConcretePartialImplementationOfWildcardType:
//		  (class: synthetic=false, interface=false, abstract=false)
//		  (class: supertype=InterfaceImplementationUnderstandingTests.AbstractPartialImplementationWithWildcardType<Integer,DBInteger>)
//	      this.toDBvValue<Integer>(Integer): DBInteger
//	      AbstractPartialImplementationWithWildcardType.toObjectValue<AbstractPartialImplementationWithWildcardType.Q extends DBNumber>(DBNumber): AbstractPartialImplementationWithWildcardType.T extends Number
// => use supertype reference for "implements MyInterface<T,Q>", before trying in supertype itself
// Hmm, this is hard. In this case, might be easier to investigate methods, but still hard work.
// Also note that we can't recurse into the supertype because that looses too much information.
// => So, now that we have the ordered lookups to "T" and "Q" in the abstract class, apply the constraints as observed here, in the same order.
// => Then derive that backwards to discover what the types are for the "implements MyInterface" declaration.
// => Alternatively: might be easier to work backwards from start: i) accept supertype reference and its values, ii) pass these up
//                   when looking up recursively towards supertypes until find an "implements MyInterface".
	
//	Methods of this=ConcretePartialImplementationOfWildcardTypeWithAgreeingInterface:
//		  (class: synthetic=false, interface=false, abstract=false)
//		  (class: supertype=InterfaceImplementationUnderstandingTests.AbstractPartialImplementationWithWildcardType<Integer,DBInteger>)
//		  (class: implements=InterfaceImplementationUnderstandingTests.MyInterface<Integer,DBInteger>)
// => use "implements MyInterface<T,Q>" before using supertype reference
	
	
//	Methods of this=AbstractPartialImplementationWithWildcardType:
//	  (class: synthetic=false, interface=false, abstract=true)
//	  (class: typeVariables=<this.T extends Number,this.Q extends DBNumber>)
// note: could have been  <this.Q extends DBNumber,this.T extends Number> without breaking the 'implements' section.
//	  (class: supertype=Object)
//	  (class: implements=InterfaceImplementationUnderstandingTests.MyInterface<this.T extends Number,this.Q extends DBNumber>)
//	
//	Methods of this=AbstractPartialReImplementationOfWildcardTypeWithWildcardType:
//		  (class: synthetic=false, interface=false, abstract=true)
//		  (class: typeVariables=<this.I extends Integer>)
//		  (class: supertype=AbstractPartialImplementationWithWildcardType<this.I extends Integer,DBInteger>)
//	
//	Methods of this=ConcretePartialReImplementationOfWildcardTypeWithWildcardType:
//		  (class: synthetic=false, interface=false, abstract=false)
//		  (class: supertype=InterfaceImplementationUnderstandingTests.AbstractPartialReImplementationOfWildcardTypeWithWildcardType<Integer>)
// => No "implements MyInterface", so start with supertype reference: grab the type parameter and pass it through
// => Then look at the supertype (AbstractPartialReImplementationOfWildcardTypeWithWildcardType)
// => It has typeVariable "I" (now supplied value "Integer")
//    That's used used in the next supertype reference (because again there's no "implements MyInterface")
// => So grab the two type parameters in the reference to the next supertype and pass them through: Integer, DBInteger
// => Then look at the next supertype (AbstractPartialImplementationWithWildcardType)
// => It has typeVariables "T" (now supplied value "Integer") and "Q" (now supplied value "DBInteger")
//    That's used in the "implements MyInterface" reference, supplying values <"T" = "Integer", "Q" = "DBInteger">
// Finally, values given to type variables are of the form: {upperBound:Type, lowerBound:Type},
// however the types used will be only GenericArrayType and Class:
//   - WildcardType will be expanded out into {upperBound:Type, lowerBound:Type},
//   - TypeVariable will be expanded out into {upperBound:Type, lowerBound:Type},
//   - ParameterizedType will not be supported and will be expanded out into <null>, which will be treated as "unknown".
	
	
	
	/**
	 * @param interfaceClass
	 * @param implementationClass must be a concrete type
	 */
	public InterfaceInfo(Class<?> interfaceClass, Class<?> implementationClass) {
		// step 0: validation: interfaceType must be an interface, implementationType must be a concrete class
		// can likely increase the scope later, but for now all assumptions are based on this
		if (!interfaceClass.isInterface()) {
			throw new IllegalArgumentException("interfaceType must be an interface");
		}
		if (implementationClass.isInterface() || Modifier.isAbstract(implementationClass.getModifiers())) {
			throw new IllegalArgumentException("implementationType must be a concrete class");
		}
		this.interfaceClass = interfaceClass;
		this.implementationClass = implementationClass;
		
		// step 1: examine the 'implements' section and see if the implementation actually implements the interface
		//System.out.println(); implementationType.getSuperclass()
		Type interfaceType = null; // Type that matches up with 'interfaceClass'
		Type[] implementedInterfaces = implementationClass.getGenericInterfaces();
		for (Type implementedInterface: implementedInterfaces) {
			Class<?> implementedInterfaceClass = resolveClassOf(implementedInterface);
			if (implementedInterfaceClass.equals(interfaceClass)) {
				interfaceType = implementedInterface;
				this.interfaceImplementedByImplementation = true;
				break;
			}
		}
		
		// step 2: examine the 'implements' section in reference to the given interface type,
		//         and resolve type variables to classes (using null for class where can't resolve)
		if (interfaceType != null) {
			// question: how do I track which TypeVariable goes with which parameter/return type on methods?
			// Think I need to investigate the interface type itself.
			
			// TODO
		}
		
		// step 3: examine the non-interface, non-abstract methods and find the methods that relate
		//         to the resolved type variables (storing null method where can't figure it out)
		if (interfaceImplementedByImplementation) {
			// TODO
		}
	}
	
	// supports only classes and parameterized types
	// doesn't support Array[] types or wildcard types
	protected static Class<?> resolveClassOf(Type type) {
		if (type instanceof Class<?>) {
			return (Class<?>)type;
		}
		else if (type instanceof ParameterizedType) {
			return resolveClassOf(((ParameterizedType) type).getRawType());
		}
		else {
			throw new UnsupportedOperationException("Can't yet handle "+type.getClass().getSimpleName()+" types");
		}
	}
	
	/**
	 * Indicates whether the implementation type actually makes any attempt to implement the interface type.
	 * @return
	 */
	public boolean isInterfaceImplementedByImplementation() {
		return interfaceImplementedByImplementation;
	}
	
	/**
	 * Gets the methods on the implementation type that are the concrete
	 * implementation methods of the interface.
	 * @return
	 */
	public List<Method> getImplementationMethods() {
		// TODO
		return null;
	}

	/**
	 * Gets the specified method on the implementation type that is the concrete
	 * implementation method of the interface.
	 * 
	 * <p> If couldn't figure out which method implements the interface, this will return null.
	 * @param name name of method in interface type
	 * @param parameterTypes parameters of method in interface type 
	 * @return null
	 * @throws IllegalArgumentException if no such method
	 */
	public Method getImplementationMethod(String name, Class<?>... parameterTypes) {
		Method refMethod;
		try {
			refMethod = interfaceClass.getMethod(name, parameterTypes);
		} catch (SecurityException e) {
			throw new DBRuntimeException("Security exception attempting to retrieve method "+name+" on "+
					interfaceClass.getName()+": "+e.getLocalizedMessage(), e);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException("Method "+name+" is not present on "+interfaceClass.getName()+" with the specified parameters", e);
		}
		
		return implementationMethodsByInterfaceMethod.get(refMethod);
	}

	/**
	 * Gets the specified method on the implementation type that is the concrete
	 * implementation method of the interface.
	 * 
	 * <p> This method retrieves the method by its name (on the interface class) alone.
	 * It cannot be used if the method name is overloaded on the interface.
	 * Note: it doesn't matter if the method is overloaded in the implementation class.
	 * 
	 * <p> If couldn't figure out which method implements the interface, this will return null.
	 * @param name name of non-overloaded method in interface type
	 * @return null
	 * @throws IllegalArgumentException if no such method or method is overloaded
	 */
	public Method getImplementationMethodByName(String name) {
		Method refMethod = null;
		for (Method interfaceMethod: interfaceClass.getMethods()) {
			if (interfaceMethod.getName().equals(name)) {
				if (refMethod == null) {
					refMethod = interfaceMethod;
				}
				else {
					throw new IllegalArgumentException("Method "+name+" is overloaded on "+interfaceClass.getName()+
							", use getImplementationMethod(name, Class[]) instead");
				}
			}
		}
		if (refMethod == null) {
			throw new IllegalArgumentException("Method "+name+" is not present on "+interfaceClass.getName());
		}

		return implementationMethodsByInterfaceMethod.get(refMethod);
	}
}
