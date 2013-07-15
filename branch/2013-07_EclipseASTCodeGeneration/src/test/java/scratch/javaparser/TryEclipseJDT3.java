package scratch.javaparser;

import java.io.File;

import nz.co.gregs.dbvolution.DBInteger;
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
public class TryEclipseJDT3 {
	public static void main(String[] args) throws MalformedTreeException, BadLocationException {
		
		ParsedClass javatype = ParsedClass.newDBTableInstance(
				TryEclipseJDT3.class.getPackage().getName()+".TryEclipseJDT3_Marque",
				"t_3");
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
}
