package scratch.javaparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.generation.ast.ColumnNameResolver;
import nz.co.gregs.dbvolution.generation.ast.ParsedClass;
import nz.co.gregs.dbvolution.generation.ast.ParsedField;
import nz.co.gregs.dbvolution.generation.ast.ParsedMethod;

import org.eclipse.jface.text.BadLocationException;
import org.eclipse.text.edits.MalformedTreeException;

/**
 * @see http://help.eclipse.org/helios/index.jsp?topic=%2Forg.eclipse.jdt.doc.isv%2Freference%2Fapi%2Forg%2Feclipse%2Fjdt%2Fcore%2Fdom%2FASTParser.html
 * @author Malcolm Lett
 */
public class TryEclipseJDT2 {
	public static void main(String[] args) throws MalformedTreeException, BadLocationException {
		ParsedClass javatype = ParsedClass.of(getSource());
		System.out.println(javatype.toString());
		
		ColumnNameResolver columnNameResolver = new ColumnNameResolver();
		ParsedField newField = ParsedField.newDBTableColumnInstance(javatype.getTypeContext(),
				columnNameResolver.getPropertyNameFor("c_2"), DBInteger.class, false, "c_2");
		System.out.println(newField);
		javatype.addFieldAfter(null, newField);
		
		ParsedMethod newMethod = ParsedMethod.newFieldGetterInstance(javatype.getTypeContext(), newField);
		System.out.println(newMethod);
		javatype.addMethodAfter(null, newMethod);
		
		File srcFolder = new File("target/test-output");
		srcFolder.mkdirs();
		javatype.writeToSourceFolder(srcFolder);
	}
	
	private static String getSource() {
		BufferedReader reader = null;
		try {
			StringBuilder buf = new StringBuilder();
			//InputStream is = TryEclipseJDT.class.getResourceAsStream("SampleSimpleJava.txt");
			InputStream is = TryEclipseJDT2.class.getResourceAsStream("Marque.java.txt");
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
}
