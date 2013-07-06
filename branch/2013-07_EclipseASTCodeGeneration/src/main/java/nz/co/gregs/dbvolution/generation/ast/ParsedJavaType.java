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

import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.ASTParser;
import org.eclipse.jdt.core.dom.AbstractTypeDeclaration;
import org.eclipse.jdt.core.dom.BodyDeclaration;
import org.eclipse.jdt.core.dom.ChildListPropertyDescriptor;
import org.eclipse.jdt.core.dom.CompilationUnit;
import org.eclipse.jdt.core.dom.FieldDeclaration;
import org.eclipse.jdt.core.dom.MethodDeclaration;
import org.eclipse.jdt.core.dom.TypeDeclaration;
import org.eclipse.jface.text.BadLocationException;
import org.eclipse.jface.text.Document;
import org.eclipse.text.edits.MalformedTreeException;
import org.eclipse.text.edits.TextEdit;

public class ParsedJavaType {
	private final ASTParser parser;
	private final Document document;
	private final CompilationUnit unit;
	private final ParsedTypeContext typeContext;
	private TypeDeclaration astNode;
	private List<ParsedField> fields;
	private List<ParsedMethod> methods;
	
	public static ParsedJavaType of(String contents) {
		ASTParser parser = ASTParser.newParser(AST.JLS4);
		// In order to parse 1.5 code, some compiler options need to be set to 1.5
		Map<?,?> options = JavaCore.getOptions();
		JavaCore.setComplianceOptions(JavaCore.VERSION_1_5, options);
		parser.setCompilerOptions(options);
		 
		Document document = new Document(contents);
		parser.setSource(document.get().toCharArray());
		CompilationUnit unit = (CompilationUnit)parser.createAST(null);
		unit.recordModifications();
		
		return new ParsedJavaType(parser, document, unit);
	}

	/**
	 * Builds up the high-level model of the contents of the java type. 
	 */
	public ParsedJavaType(ASTParser parser, Document document, CompilationUnit unit) {
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
	    	throw new RuntimeException("found no type");
	    }

	    // fields
		this.fields = new ArrayList<ParsedField>();
		for (BodyDeclaration body: (List<BodyDeclaration>)astNode.bodyDeclarations()) {
			if (body.getNodeType() == ASTNode.FIELD_DECLARATION) {
				fields.add(new ParsedField(typeContext, (FieldDeclaration)body));
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
	
	public ParsedTypeContext getTypeContext() {
		return typeContext;
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
				writer.close();
			} catch (IOException dropped) {} // assume caused by earlier exception
		}
	}
	
	public String getName() {
		return astNode.getName().getFullyQualifiedName();
	}
	
	public String getPackage() {
		return unit.getPackage().getName().getFullyQualifiedName();
	}
	
	public String getFullyQualifiedName() {
		return getPackage()+"."+getName();
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
