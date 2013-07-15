package nz.co.gregs.dbvolution.generation.ast;

import java.util.Comparator;

import org.eclipse.jdt.core.dom.ImportDeclaration;

/**
 * Defines the standard order for imports:
 * <ul>
 * <li> static imports first, sorted according to the remainder of the rules
 * <li> java.* first, then javax.*, then all the rest
 * <li> alphabetical sorting applied otherwise
 * </ul>
 * 
 * <p> Capable of comparing a number of different object types at the same
 * time:
 * <ul>
 * <li> JDT ImportDeclaration
 * <li> Class
 * <li> String (fully qualified name of type)
 * </ul>
 * @author Malcolm Lett
 */
class ImportComparator implements Comparator<Object> {

	@Override
	public int compare(Object importOrType1, Object importOrType2) {
		// static first
		if (isStatic(importOrType1) ^ isStatic(importOrType2)){ 
			return isStatic(importOrType1) ? -1 : +1;
		}
		
		String package1 = fullyQualifiedNameOf(importOrType1);
		String package2 = fullyQualifiedNameOf(importOrType2);
		
		// java.* first
		if (isJavaPackage(package1) ^ isJavaPackage(package2)) {
			return isJavaPackage(package1) ? -1 : +1;
		}
		
		// of remainder, javax.* first
		if (isJavaxPackage(package1) ^ isJavaxPackage(package2)) {
			return isJavaxPackage(package1) ? -1 : +1;
		}
		
		// of remainder, alphabetical ordering
		return package1.compareTo(package2);
	}
	
	private boolean isJavaPackage(String packageName) {
		return packageName.startsWith("java.");
	}

	private boolean isJavaxPackage(String packageName) {
		return packageName.startsWith("javax.");
	}
	
	private boolean isStatic(Object obj) {
		if (obj instanceof ImportDeclaration) {
			return ((ImportDeclaration) obj).isStatic();
		}
		else if (obj instanceof Class) {
			return false;
		}
		else if (obj instanceof String) {
			return false;
		}
		else {
			throw new IllegalArgumentException("Unsupported type: "+obj.getClass().getName());
		}
	}

	private String fullyQualifiedNameOf(Object obj) {
		if (obj instanceof ImportDeclaration) {
			return fullyQualifiedNameOf((ImportDeclaration) obj);
		}
		else if (obj instanceof Class) {
			return fullyQualifiedNameOf((Class<?>) obj);
		}
		else if (obj instanceof String) {
			return (String) obj;
		}
		else {
			throw new IllegalArgumentException("Unsupported type: "+obj.getClass().getName());
		}
	}
	
	private String fullyQualifiedNameOf(ImportDeclaration importDeclaration) {
		return importDeclaration.getName().getFullyQualifiedName();
	}
	
	private String fullyQualifiedNameOf(Class<?> type) {
		return type.getName();
	}

}
