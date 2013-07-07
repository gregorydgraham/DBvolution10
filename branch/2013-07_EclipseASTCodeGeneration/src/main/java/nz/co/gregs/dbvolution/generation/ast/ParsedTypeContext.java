package nz.co.gregs.dbvolution.generation.ast;

import java.util.List;

import nz.co.gregs.dbvolution.annotations.DBSelectQuery;
import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTableForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;

import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.CompilationUnit;
import org.eclipse.jdt.core.dom.ImportDeclaration;

import scratch.javaparser.TryEclipseJDT;

/**
 * Information available to types, fields, members within a java source file.
 * @author Malcolm Lett
 */
public class ParsedTypeContext {
	private static final Class<?>[] KNOWN_IMPORTABLE_TYPES = {
		DBSelectQuery.class, DBTableColumn.class, DBTableForeignKey.class,
		DBTableName.class, DBTablePrimaryKey.class}; // TODO: ideally pick this up somehow automatically
	private CompilationUnit unit;
	
	public ParsedTypeContext(CompilationUnit unit) {
		this.unit = unit;
	}
	
	public AST getAST() {
		return unit.getAST();
	}
	
	// TODO: keep order of imports following standards
	/**
	 * Attempts to add the import if it is missing.
	 * If the import can't be added because an import already exists with the
	 * same unqualified name, then  
	 * @param type
	 * @return {@code true} if import included afterwards, {@code false} if can't add due to duplicate name
	 */
	public boolean ensureImport(Class<?> type) {
		String typeSimpleName = type.getSimpleName();
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
				else if (!importDecl.isOnDemand() &&
						(importDecl.getName().getFullyQualifiedName().endsWith("."+typeSimpleName) ||
								importDecl.getName().getFullyQualifiedName().equals(typeSimpleName))) {
					// already contains a conflicting simple name
					return false;
				}
			}
		}
		
		AST ast = unit.getAST();
	    ImportDeclaration id = ast.newImportDeclaration();
	    String classToImport = TryEclipseJDT.class.getName();
	    id.setName(ast.newName(type.getName()));
	    unit.imports().add(id); // add import declaration at end
	    return true;
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
