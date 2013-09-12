package nz.co.gregs.dbvolution.generation.ast;

import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBSelectQuery;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.generation.CodeGenerationConfiguration;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.CompilationUnit;
import org.eclipse.jdt.core.dom.ImportDeclaration;
import org.eclipse.jdt.core.dom.Name;
import org.eclipse.jdt.core.dom.PackageDeclaration;
import org.eclipse.jdt.core.dom.Type;

/**
 * Information available to types, fields, members within a java source file.
 * @author Malcolm Lett
 */
public class ParsedTypeContext {
	private static final Class<?>[] KNOWN_IMPORTABLE_TYPES = {
		DBSelectQuery.class, DBColumn.class, DBForeignKey.class,
		DBTableName.class, DBPrimaryKey.class}; // TODO: ideally pick this up somehow automatically
	private CodeGenerationConfiguration config = new CodeGenerationConfiguration();
	private CompilationUnit unit;
	
	public static ParsedTypeContext newInstance(String packageName) {
		AST ast = AST.newAST(AST.JLS4);
		ParsedTypeContext typeContext = new ParsedTypeContext(ast.newCompilationUnit());
		typeContext.setPackage(packageName);
		return typeContext;
	}
	
	public ParsedTypeContext(CompilationUnit unit) {
		this.unit = unit;
	}
	
	public AST getAST() {
		return unit.getAST();
	}
	
	/**
	 * @return the config
	 */
	public CodeGenerationConfiguration getConfig() {
		return config;
	}

	/**
	 * @param config the config to set
	 */
	public void setConfig(CodeGenerationConfiguration config) {
		this.config = config;
	}

	public CompilationUnit unitAstNode() {
		return unit;
	}
	
	/**
	 * 
	 * @return null if no package
	 */
	public String getPackage() {
		PackageDeclaration packageDeclaration = unit.getPackage();
		if (packageDeclaration != null) {
			return packageDeclaration.getName().getFullyQualifiedName();
		}
		return null;
	}
	
	public void setPackage(String name) {
		PackageDeclaration packageDeclaration = unit.getPackage();
		if (name != null) {
			if (packageDeclaration == null) {
				packageDeclaration = getAST().newPackageDeclaration();
				unit.setPackage(packageDeclaration);
			}
			packageDeclaration.setName(getAST().newName(name));
		}
		else if (name == null) {
			unit.setPackage(null);
		}
	}
	
	/**
	 * Gets the type name that can be used as a declaration of the
	 * given type in generated code.
	 * To avoid using fully qualified names, call {@link #ensureImport(Class)}
	 * prior to calling this method.
	 * @param type type to reference
	 * @return fully qualified or simple name, depending on whether the type is imported
	 */
	public Name declarableTypeNameOf(Class<?> type) {
		return declarableTypeNameOf(type, false);
	}
	
	/**
	 * Gets the type name that can be used as a declaration of the
	 * given type in generated code.
	 * To avoid using fully qualified names, call {@link #ensureImport(Class)}
	 * prior to calling this method.
	 * @param type type to reference
	 * @return fully qualified or simple name, depending on whether the type is imported
	 */
	public Type declarableTypeOf(Class<?> type) {
		return declarableTypeOf(type, false);
	}

	/**
	 * Gets the type name that can be used as a declaration of the
	 * given type in generated code.
	 * If requested, attempts to add the type to the imports section
	 * if not already present.
	 * 
	 * <p> Note: this does not support complex type references
	 * or primitive types.
	 * @param type type to reference
	 * @param addImport whether or not to add imports
	 * @return fully qualified or simple name, depending on whether the type is imported
	 */
	public Name declarableTypeNameOf(Class<?> type, boolean addImport) {
		if (addImport) {
			ensureImport(type);
		}
		return getAST().newSimpleName(
				isImported(type) ? type.getSimpleName() : type.getName());
	}
	
	/**
	 * Gets the type name that can be used as a declaration of the
	 * given type in generated code.
	 * If requested, attempts to add the type to the imports section
	 * if not already present.
	 * 
	 * <p> Note: this does not support complex type references
	 * or primitive types.
	 * @param type type to reference
	 * @param addImport whether or not to add imports
	 * @return fully qualified or simple name, depending on whether the type is imported
	 */
	public Type declarableTypeOf(Class<?> type, boolean addImport) {
		if (addImport) {
			ensureImport(type);
		}
		return getAST().newSimpleType(
			getAST().newSimpleName(
				isImported(type) ? type.getSimpleName() : type.getName()));
	}
	
	/**
	 * Indicates whether or not the type has been imported.
	 * @param type
	 * @return
	 */
	public boolean isImported(Class<?> type) {
		for (ImportDeclaration importDecl: (List<ImportDeclaration>)unit.imports()) {
			if (!importDecl.isStatic()) {
				if (!importDecl.isOnDemand() &&
						importDecl.getName().getFullyQualifiedName().equals(type.getName())) {
					// already present
					return true;
				}
				else if (importDecl.isOnDemand() &&
						importDecl.getName().getFullyQualifiedName().equals(type.getPackage().getName())) {
					// package wildcard already present
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Attempts to add the import if it is missing.
	 * If the import can't be added because an import already exists with the
	 * same unqualified name, then the full type reference must be used inline.
	 * 
	 * <p> Imports are added according to standard ordering.
	 * @param type
	 * @return {@code true} if import included afterwards, {@code false} if can't add due to duplicate name
	 */
	public boolean ensureImport(Class<?> type) {
		// check if already imported
		if (isImported(type)) {
			return true;
		}

		// check if contains conflicting imports
		String typeSimpleName = type.getSimpleName();
		for (ImportDeclaration importDecl: (List<ImportDeclaration>)unit.imports()) {
			if (!importDecl.isStatic()) {
				if (!importDecl.isOnDemand() &&
						(importDecl.getName().getFullyQualifiedName().endsWith("."+typeSimpleName) ||
								importDecl.getName().getFullyQualifiedName().equals(typeSimpleName))) {
					// already contains a conflicting simple name
					return false;
				}
			}
		}
		
		AST ast = unit.getAST();
	    ImportDeclaration importDeclaration = ast.newImportDeclaration();
	    importDeclaration.setName(ast.newName(type.getName()));
	    addImport(importDeclaration);
	    return true;
	}
	
	/**
	 * Adds the import, maintaining preferred order.
	 * @param newImport
	 * @return
	 */
	protected void addImport(ImportDeclaration newImport) {
		int targetIndex = -1;
		int index = 0;
		ImportComparator order = new ImportComparator();
		for (ImportDeclaration currentImport: (List<ImportDeclaration>) unit.imports()) {
			if (order.compare(newImport, currentImport) < 0) {
				targetIndex = index;
				break;
			}
			index++;
		}
		if (targetIndex == -1) {
			targetIndex = index;
		}
		
		// add before next in order, or at end
		unit.imports().add(targetIndex, newImport);
	}
	
	/**
	 * Checks if the specified {@code declaredTypeName} is the given type,
	 * according to the imports available.
	 * This method handles wildcard imports accurately (although
	 * it assumes the source file is correct and does not have overlapping
	 * imports).
	 * @param type the reference type
	 * @param declaredTypeName the declared type name to check
	 * @return
	 */
	public boolean isDeclarationOfType(Class<?> type, String declaredTypeName) {
		// check for already-fully-qualified match
		if (type.getName().equals(declaredTypeName)) {
			return true;
		}
		
		// check simple names match
		String simpleDeclaredTypeName = declaredTypeName;
		if (declaredTypeName.contains(".")) {
			simpleDeclaredTypeName = declaredTypeName.substring(declaredTypeName.lastIndexOf(".")+1);
		}
		if (!type.getSimpleName().equals(simpleDeclaredTypeName)) {
			return false; // not even possible
		}
		
		// check against imports
		// (if 'type' is imported and its sample name matches the declared name, then we've found a match)
		// (TODO: think it should require that 'declaredTypeName' is only a simple name) 
		for (ImportDeclaration importDecl: (List<ImportDeclaration>)unit.imports()) {
			if (!importDecl.isStatic()) {
				if (!importDecl.isOnDemand() &&
						importDecl.getName().getFullyQualifiedName().equals(type.getName())) {
					return true;
				}
				else if (importDecl.isOnDemand() &&
						importDecl.getName().getFullyQualifiedName().equals(type.getPackage().getName())) {
					// package wildcard present
					return true;
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Attempts to infer the fully qualified name of the given type,
	 * or otherwise just returns the same name given.
	 * Looks up the imports to fully qualify the given name.
	 * Note: the current implementation can't handle wildcard imports.
	 * @param name
	 * @return
	 */
	public String getFullyQualifiedNameOf(String name) {
		// already fully qualified
		if (name.contains(".")) {
			return name;
		}
		
		// lookup imports
		for (ImportDeclaration importDecl: (List<ImportDeclaration>)unit.imports()) {
			if (!importDecl.isOnDemand() && !importDecl.isStatic()) {
				String importName = importDecl.getName().getFullyQualifiedName();
				if (importName.endsWith("."+name) || importName.equals(name)) {
					return importName;
				}
			}
		}
		
		// help out when using wildcards to import DBVolution types
		for (Class<?> importableType: KNOWN_IMPORTABLE_TYPES) {
			if (importableType.getSimpleName().equals(name)) {
				// look for a wildcard import for this known type
				for (ImportDeclaration importDecl: (List<ImportDeclaration>)unit.imports()) {
					if (importDecl.isOnDemand() && !importDecl.isStatic()) {
						String importName = importDecl.getName().getFullyQualifiedName();
						if (importName.equals(importableType.getPackage().getName())) {
							return importableType.getName();
						}
					}
				}
			}
		}
		
		// give up - hope it doesn't matter
		return name;
	}
}
