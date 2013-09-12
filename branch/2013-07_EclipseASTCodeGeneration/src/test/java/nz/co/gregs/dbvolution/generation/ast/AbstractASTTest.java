package nz.co.gregs.dbvolution.generation.ast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class AbstractASTTest {
	protected static String getMarqueSource() {
		BufferedReader reader = null;
		try {
			StringBuilder buf = new StringBuilder();
			InputStream is = LowLevelGenerationTests.class.getResourceAsStream("Marque.java.txt");
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
