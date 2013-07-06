package scratch.javaparser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.core.dom.AST;
import org.eclipse.jdt.core.dom.ASTNode;
import org.eclipse.jdt.core.dom.ASTParser;
import org.eclipse.jdt.core.dom.AbstractTypeDeclaration;
import org.eclipse.jdt.core.dom.BodyDeclaration;
import org.eclipse.jdt.core.dom.CompilationUnit;
import org.eclipse.jdt.core.dom.FieldDeclaration;
import org.eclipse.jdt.core.dom.IExtendedModifier;
import org.eclipse.jdt.core.dom.ImportDeclaration;
import org.eclipse.jdt.core.dom.MethodDeclaration;
import org.eclipse.jdt.core.dom.VariableDeclarationFragment;
import org.eclipse.jface.text.BadLocationException;
import org.eclipse.jface.text.Document;
import org.eclipse.text.edits.MalformedTreeException;
import org.eclipse.text.edits.TextEdit;

/**
 * @see http://help.eclipse.org/helios/index.jsp?topic=%2Forg.eclipse.jdt.doc.isv%2Freference%2Fapi%2Forg%2Feclipse%2Fjdt%2Fcore%2Fdom%2FASTParser.html
 * @author Malcolm Lett
 */
public class TryEclipseJDT {
	public static void main(String[] args) throws MalformedTreeException, BadLocationException {
		System.out.println("Hello world");
		
		ASTParser parser = ASTParser.newParser(AST.JLS4);
		 // In order to parse 1.5 code, some compiler options need to be set to 1.5
		 Map<?,?> options = JavaCore.getOptions();
		 JavaCore.setComplianceOptions(JavaCore.VERSION_1_5, options);
		 parser.setCompilerOptions(options);
		 
		Document document = new Document(getSource());
		parser.setSource(document.get().toCharArray());
		CompilationUnit unit = (CompilationUnit)parser.createAST(null);
		unit.recordModifications();
		
	    // to get the imports from the file
	    List<ImportDeclaration> imports = unit.imports();
	    for (ImportDeclaration i : imports) {
	        System.out.println(i.getName().getFullyQualifiedName());
	    }
	    
	    // to iterate through methods
	    List<AbstractTypeDeclaration> types = unit.types();
	    for (AbstractTypeDeclaration type : types) {
	        if (type.getNodeType() == ASTNode.TYPE_DECLARATION) {
	            // Class def found
	            List<BodyDeclaration> bodies = type.bodyDeclarations();
	            for (BodyDeclaration body : bodies) {
	                if (body.getNodeType() == ASTNode.METHOD_DECLARATION) {
	                    MethodDeclaration method = (MethodDeclaration)body;
	                    //method.get
	                    System.out.println("name("+method.getStartPosition()+"+"+method.getLength()+"): " + method.getName().getFullyQualifiedName());
	                    System.out.println("       modifiers: "+method.getModifiersProperty());
	                    for (Object key: method.properties().keySet()) {
	                    	System.out.println("  property: "+key+" = "+method.properties().get(key));
	                    }
	                }
	    	        if (body.getNodeType() == ASTNode.FIELD_DECLARATION) {
	    	        	FieldDeclaration field = (FieldDeclaration)body;
	    	        	for (VariableDeclarationFragment variable: (List<VariableDeclarationFragment>)field.fragments()) {
	    	        		System.out.println("name: "+variable.getName());
	    	        	}
	    	        	List<IExtendedModifier> modifiers = field.modifiers();
	    	        	for(IExtendedModifier modifier: modifiers) {
	    	        		System.out.println("  modifier: "+modifier);
	    	        	}
	    	        }
	            }
	        }
	    }	    
	    
	    // to create a new import
	    AST ast = unit.getAST();
	    ImportDeclaration id = ast.newImportDeclaration();
	    String classToImport = TryEclipseJDT.class.getName();
	    id.setName(ast.newName(classToImport.split("\\.")));
	    //unit.imports().add(id); // add import declaration at end
	    unit.imports().add(3, id);

	    // to save the changed file
	    TextEdit edits = unit.rewrite(document, null);
	    edits.apply(document);
	    writeResult(document.get());
	}
	
	private static String getSource() {
		BufferedReader reader = null;
		try {
			StringBuilder buf = new StringBuilder();
			//InputStream is = TryEclipseJDT.class.getResourceAsStream("SampleSimpleJava.txt");
			InputStream is = TryEclipseJDT.class.getResourceAsStream("Marque.java.txt");
			reader = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = reader.readLine()) != null) {
				buf.append(line).append("\n");
			}
			return buf.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				reader.close();
			} catch (IOException dropped) {} // assume caused by earlier exception
		}
	}
	
	private static void writeResult(String contents) {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(new File("target/result.java")));
			writer.write(contents);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				writer.close();
			} catch (IOException dropped) {} // assume caused by earlier exception
		}
	}
}
