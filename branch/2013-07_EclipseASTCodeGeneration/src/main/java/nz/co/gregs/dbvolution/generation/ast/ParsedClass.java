package nz.co.gregs.dbvolution.generation.ast;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBTableName;

import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.ASTParser;
import org.eclipse.jdt.core.dom.AbstractTypeDeclaration;
import org.eclipse.jdt.core.dom.BodyDeclaration;
import org.eclipse.jdt.core.dom.CompilationUnit;
import org.eclipse.jdt.core.dom.FieldDeclaration;
import org.eclipse.jdt.core.dom.MethodDeclaration;
import org.eclipse.jdt.core.dom.Modifier;
import org.eclipse.jdt.core.dom.Name;
import org.eclipse.jdt.core.dom.PackageDeclaration;
import org.eclipse.jdt.core.dom.SingleMemberAnnotation;
import org.eclipse.jdt.core.dom.StringLiteral;
import org.eclipse.jdt.core.dom.TagElement;
import org.eclipse.jdt.core.dom.TextElement;
import org.eclipse.jdt.core.dom.TypeDeclaration;
import org.eclipse.jface.text.BadLocationException;
import org.eclipse.jface.text.Document;
import org.eclipse.text.edits.MalformedTreeException;
import org.eclipse.text.edits.TextEdit;

/**
 * The parsed details of a class within a source file.
 * @author Malcolm Lett
 */
// TODO: consider making this type transparently expose a single instance of a type
// within a multi-type compilation unit, in the same way as planned for ParsedFields.s
public class ParsedClass {
	private final ASTParser parser;
	private final Document document;
	private final CompilationUnit unit;
	private final ParsedTypeContext typeContext;
	private TypeDeclaration astNode;
	private ParsedTypeRef superType;
	private List<ParsedField> fields;
	private List<ParsedMethod> methods;
	
	/**
	 * Parses an existing source file.
	 */
	public static ParsedClass of(String contents) {
		ASTParser parser = ASTParser.newParser(AST.JLS4);
		// In order to parse 1.5 code, some compiler options need to be set to 1.5
		Map<?,?> options = JavaCore.getOptions();
		JavaCore.setComplianceOptions(JavaCore.VERSION_1_5, options);
		parser.setCompilerOptions(options);
		 
		Document document = new Document(contents);
		parser.setSource(document.get().toCharArray());
		CompilationUnit unit = (CompilationUnit)parser.createAST(null);
		unit.recordModifications();
		
		return new ParsedClass(parser, document, unit);
	}

	/**
	 * Creates a brand new type for a brand new source file.
	 * @param fullyQualifiedName
	 * @return
	 */
	public static ParsedClass newInstance(String fullyQualifiedName) {
		String packageName = null;
		String simpleName = fullyQualifiedName;
		if (fullyQualifiedName.contains(".")) {
			simpleName = fullyQualifiedName.substring(fullyQualifiedName.lastIndexOf(".")+1);
		}
		if (fullyQualifiedName.contains(".")) {
			packageName = fullyQualifiedName.substring(0, fullyQualifiedName.lastIndexOf("."));
		}
		return newInstance(packageName, simpleName);
	}
	
	/**
	 * Creates a brand new type for a brand new source file.
	 * @param packageName
	 * @param simpleName
	 * @return
	 */
	public static ParsedClass newInstance(String packageName, String simpleName) {
		ASTParser parser = ASTParser.newParser(AST.JLS4);
		// In order to parse 1.5 code, some compiler options need to be set to 1.5
		Map<?,?> options = JavaCore.getOptions();
		JavaCore.setComplianceOptions(JavaCore.VERSION_1_5, options);
		parser.setCompilerOptions(options);
		
		Document document = new Document("");
		parser.setSource(document.get().toCharArray());
		CompilationUnit unit = (CompilationUnit)parser.createAST(null);
		unit.recordModifications();
		
		AST ast = unit.getAST();		
		
		// create type
		TypeDeclaration typeDeclaration = ast.newTypeDeclaration();
		typeDeclaration.setName(ast.newSimpleName(simpleName));
		unit.types().add(typeDeclaration);
		
		// set visibility modifiers
		typeDeclaration.modifiers().add(ast.newModifier(Modifier.ModifierKeyword.PUBLIC_KEYWORD));

		// set package
		if (packageName != null) {
			PackageDeclaration packageDeclaration = ast.newPackageDeclaration();
			packageDeclaration.setName(ast.newName(packageName));
			unit.setPackage(packageDeclaration);
		}
		
		return new ParsedClass(parser, document, unit);
	}

	/**
	 * Creates a brand new type for a brand new source file.
	 * @param fullyQualifiedClassName
	 * @param tableName
	 * @return
	 */
	public static ParsedClass newDBTableInstance(String fullyQualifiedClassName, String tableName) {
		String packageName = null;
		String simpleClassName = fullyQualifiedClassName;
		if (fullyQualifiedClassName.contains(".")) {
			simpleClassName = fullyQualifiedClassName.substring(fullyQualifiedClassName.lastIndexOf(".")+1);
		}
		if (fullyQualifiedClassName.contains(".")) {
			packageName = fullyQualifiedClassName.substring(0, fullyQualifiedClassName.lastIndexOf("."));
		}
		return newDBTableInstance(packageName, simpleClassName, tableName);
	}
	
	/**
	 * Creates a brand new type for a brand new source file.
	 * @param packageName
	 * @param simpleClassName
	 * @param tableName
	 * @return
	 */
	public static ParsedClass newDBTableInstance(String packageName, String simpleClassName, String tableName) {
		ParsedClass parsedClass = newInstance(packageName, simpleClassName);
		
		ParsedTypeContext typeContext = parsedClass.getTypeContext();
		AST ast = typeContext.getAST();
		TypeDeclaration typeDeclaration = parsedClass.astNode();
		
		// super type
		parsedClass.setSuperType(ParsedTypeRef.newClassInstance(typeContext, DBRow.class));

		// add @DBTableName
		ParsedTypeRef dbTableNameType = ParsedTypeRef.newClassInstance(typeContext, DBTableName.class);
		
		StringLiteral annotationValue = ast.newStringLiteral();
		annotationValue.setLiteralValue(tableName);
		SingleMemberAnnotation annotation = ast.newSingleMemberAnnotation();
		annotation.setTypeName((Name) ASTNode.copySubtree(ast, dbTableNameType.nameAstNode()));
		annotation.setValue(annotationValue);
		typeDeclaration.modifiers().add(0, annotation); // add before visibility modifiers
		
		// add javadoc
		TextElement javadocText1 = ast.newTextElement();
		javadocText1.setText("Auto-generated code. Modify at leisure.");
		TagElement javadocTag1 = ast.newTagElement();
		javadocTag1.fragments().add(javadocText1);

		TextElement javadocText2 = ast.newTextElement();
		javadocText2.setText("Subsequent auto-generations will retain modified code wherever possible.");
		TagElement javadocTag2 = ast.newTagElement();
		javadocTag2.fragments().add(javadocText2);
		
		typeDeclaration.setJavadoc(ast.newJavadoc());
		typeDeclaration.getJavadoc().tags().add(javadocTag1);
		typeDeclaration.getJavadoc().tags().add(javadocTag2);
		
		return parsedClass;
	}
	
	
	/**
	 * Builds up the high-level model of the contents of the java type. 
	 */
	public ParsedClass(ASTParser parser, Document document, CompilationUnit unit) {
		this.parser = parser;
		this.document = document;
		this.unit = unit;
		this.typeContext = new ParsedTypeContext(unit);
		
	    List<AbstractTypeDeclaration> types = unit.types();
	    for (AbstractTypeDeclaration type : types) {
	        if (type.getNodeType() == ASTNode.TYPE_DECLARATION) {
	        	if (astNode != null) {
	        		throw new RuntimeException("Not able to handle multiple types in the same file yet");
	        	}
	        	astNode = (TypeDeclaration)type;
	        }
	    }
	    if (astNode == null) {
	    	AST ast = unit.getAST();
			TypeDeclaration typeDeclaration = ast.newTypeDeclaration();
			typeDeclaration.setName(ast.newSimpleName("Name"));
			unit.types().add(typeDeclaration);
	    	astNode = typeDeclaration;
	    	
	    	//throw new RuntimeException("found no type");
	    }
	    
	    // inheritance
	    this.superType = (astNode.getSuperclassType() == null) ? null :
	    	new ParsedTypeRef(typeContext, astNode.getSuperclassType());

	    // fields
		this.fields = new ArrayList<ParsedField>();
		for (BodyDeclaration body: (List<BodyDeclaration>)astNode.bodyDeclarations()) {
			if (body.getNodeType() == ASTNode.FIELD_DECLARATION) {
				fields.addAll(ParsedField.of(typeContext, (FieldDeclaration)body));
			}
		}

	    // methods
		this.methods = new ArrayList<ParsedMethod>();
		for (BodyDeclaration body: (List<BodyDeclaration>)astNode.bodyDeclarations()) {
			if (body.getNodeType() == ASTNode.METHOD_DECLARATION) {
				methods.add(new ParsedMethod(typeContext, (MethodDeclaration)body));
			}
		}
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		String indent = "   ";
		buf.append("class "+getFullyQualifiedName()+" {").append("\n");
		for (ParsedField field: getFields()) {
			buf.append("\n"); // blank line separator
			buf.append(indent(field.toString(),indent)).append("\n");
		}
		for (ParsedMethod method: getMethods()) {
			buf.append("\n"); // blank line separator
			buf.append(indent(method.toString(),indent)).append("\n");
		}
		buf.append("}");
		return buf.toString();
	}
	
	public TypeDeclaration astNode() {
		return astNode;
	}
	
	public ParsedTypeContext getTypeContext() {
		return typeContext;
	}
	
	public void setSuperType(ParsedTypeRef superType) {
		astNode.setSuperclassType(superType.astNode());
		this.superType = superType;
	}
	
	public String getDBTableNameIfSet() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Adds a new field after the specified reference field.
	 * If no {@code after} field is specified, then adds after all
	 * other fields.
	 * If no fields are present, then adds before methods.
	 * @param after may be null
	 * @param newField the field to add
	 */
	public void addFieldAfter(ParsedField after, ParsedField newField) {
		Integer refFieldPos = null;
		
		// find reference field
		int fieldPos = 0;
		for (BodyDeclaration body: (List<BodyDeclaration>)astNode.bodyDeclarations()) {
			if (body.getNodeType() == ASTNode.FIELD_DECLARATION) {
				if (after == null || body == after.astNode()) {
					refFieldPos = fieldPos;
				}
			}
			fieldPos++;
		}
		
		// if no field found, then insert before everything else
		if (refFieldPos == null) {
			astNode.bodyDeclarations().add(0, newField.astNode());
		}
		else {
			astNode.bodyDeclarations().add(refFieldPos+1, newField.astNode());
		}
	}

	/**
	 * Adds a new method after the specified reference field.
	 * If no {@code after} method is specified, then adds after all
	 * other methods.
	 * If no methods are present, then adds after fields.
	 * @param after may be null
	 * @param newMethod the method to add
	 */
	public void addMethodAfter(ParsedMethod after, ParsedMethod newMethod) {
		Integer refMethodPos = null;
		
		// find reference field
		int contentPos = 0;
		for (BodyDeclaration body: (List<BodyDeclaration>)astNode.bodyDeclarations()) {
			if (body.getNodeType() == ASTNode.FIELD_DECLARATION) {
				if (refMethodPos == null) {
					refMethodPos = contentPos;
				}
			}
			else if (body.getNodeType() == ASTNode.METHOD_DECLARATION) {
				if (after == null || body == after.astNode()) {
					refMethodPos = contentPos;
				}
			}
			contentPos++;
		}
		
		// if no method found, then insert before everything else
		if (refMethodPos == null) {
			astNode.bodyDeclarations().add(0, newMethod.astNode());
		}
		else {
			astNode.bodyDeclarations().add(refMethodPos+1, newMethod.astNode());
		}
	}

	/**
	 * Writes the file to the appropriate sub-folder and filename within the specified
	 * source folder.
	 * @param sourceRoot root of source tree
	 */
	public void writeToSourceFolder(File sourceRoot) {
		if (!sourceRoot.exists()) {
			throw new IllegalArgumentException("Source folder does not exist: "+sourceRoot);
		}
		else if (!sourceRoot.isDirectory()) {
			throw new IllegalArgumentException("Given source folder is not a directory: "+sourceRoot);
		}
		
		String packagePath = getPackage();
		if (packagePath != null) {
			packagePath = packagePath.replace(".", File.separator);
		}
		
		File packageFolder;
		if (packagePath == null) {
			packageFolder = sourceRoot;
		}
		else {
			packageFolder = new File(sourceRoot, packagePath);
		}
		packageFolder.mkdirs();
		
		File outputFile = new File(packageFolder, getSimpleName()+".java");
		writeTo(outputFile);
	}
	
	// TODO: ensure it retains the same line endings as the original file
	public void writeTo(File file) {
	    // to save the changed file
	    TextEdit edits = unit.rewrite(document, null);
	    try {
			edits.apply(document);
		} catch (MalformedTreeException e) {
			throw new RuntimeException(e);
		} catch (BadLocationException e) {
			throw new RuntimeException(e);
		}

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(file));
			writer.write(document.get());
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException dropped) {} // assume caused by earlier exception
		}
	}
	
	public String getPackage() {
		if (unit.getPackage() != null) {
			return unit.getPackage().getName().getFullyQualifiedName();
		}
		return null;
	}

	public String getSimpleName() {
		String name = getDeclaredName();
		if (name.contains(".")) {
			return name.substring(name.lastIndexOf(".")+1);
		}
		return name;
	}
	
	public String getDeclaredName() {
		return astNode.getName().getFullyQualifiedName();
	}
	
	public String getFullyQualifiedName() {
		return ((getPackage() == null) ? "" : getPackage()+".")+getDeclaredName();
	}
	
	public List<ParsedField> getFields() {
		return fields;
	}
	
	public List<ParsedMethod> getMethods() {
		return methods;
	}
	
	/**
	 * Indents all lines within the source string by the indent string.
	 */
	private static String indent(String str, String indent) {
		try {
			StringBuilder buf = new StringBuilder();
			BufferedReader reader = new BufferedReader(new StringReader(str));
			String line;
			boolean first = true;
			while ((line = reader.readLine()) != null) {
				if (!first) buf.append("\n");
				first = false;
				buf.append(indent).append(line);
			}
			return buf.toString();
		} catch (IOException unexpected) {
			throw new RuntimeException("unexpected internal exception: "+unexpected.getMessage(), unexpected);
		}
	}
}
