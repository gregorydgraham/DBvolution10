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
