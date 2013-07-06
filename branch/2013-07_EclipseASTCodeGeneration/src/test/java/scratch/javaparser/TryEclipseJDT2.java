package scratch.javaparser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import nz.co.gregs.dbvolution.DBInteger;
import nz.co.gregs.dbvolution.generation.ast.ParsedField;
import nz.co.gregs.dbvolution.generation.ast.ParsedJavaType;

import org.eclipse.jface.text.BadLocationException;
import org.eclipse.text.edits.MalformedTreeException;

/**
 * @see http://help.eclipse.org/helios/index.jsp?topic=%2Forg.eclipse.jdt.doc.isv%2Freference%2Fapi%2Forg%2Feclipse%2Fjdt%2Fcore%2Fdom%2FASTParser.html
 * @author Malcolm Lett
 */
public class TryEclipseJDT2 {
	public static void main(String[] args) throws MalformedTreeException, BadLocationException {
		ParsedJavaType javatype = ParsedJavaType.of(getSource());
		System.out.println(javatype.toString());
		
		ParsedField newField = ParsedField.newDBTableColumnInstance(javatype.getTypeContext(), false, "c_2", DBInteger.class);
		System.out.println(newField);
		javatype.addFieldAfter(null, newField);
		
		javatype.writeTo(new File("target/"+TryEclipseJDT2.class.getSimpleName()+"-output.java"));
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
