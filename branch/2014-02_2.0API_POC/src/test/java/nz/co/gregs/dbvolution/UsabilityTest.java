package nz.co.gregs.dbvolution;

import static nz.co.gregs.dbvolution.DBQueries.*;
import org.junit.Test;

public class UsabilityTest {
	@Test
	public void defineSingleTableQueryWithoutStaticImports() {
		MyTable exemplar = DBQueries.exemplarOf(MyTable.class);
		DBQueries.where(exemplar.getUid()).permittedValues(23);
	}
	
	@Test
	public void defineSingleTableQueryWithStaticImports() {
		MyTable exemplar = exemplarOf(MyTable.class);
		where(exemplar.getName()).permittedPattern("Auckland%");
	}

	@Test
	public void defineMultiTableQueryWithStaticImports() {
		DBQuery query = new DBQuery();
		
		MyTable exemplar = exemplarOf(MyTable.class);
		where(exemplar.getName()).permittedPattern("Auckland%");
		
		MyTable2 joinedTableWithNoQuery = new MyTable2();
		query.add(exemplar, joinedTableWithNoQuery);
	}
	
	public static class MyTable extends DBRow {
		private Integer uid;
		private String name;
		public Integer getUid() {
			return uid;
		}
		public void setUid(Integer uid) {
			this.uid = uid;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
	}
	
	public static class MyTable2 extends DBRow {
		
	}

	public static class MyTable3 extends DBRow {
		
	}
}
