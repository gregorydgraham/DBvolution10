package nz.co.gregs.dbvolution.internal;

import static nz.co.gregs.dbvolution.internal.PropertyMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Date;
import java.util.List;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBAdaptType;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBTypeAdaptor;
import nz.co.gregs.dbvolution.exceptions.DBPebkacException;
import nz.co.gregs.dbvolution.internal.JavaPropertyFinder.PropertyType;
import nz.co.gregs.dbvolution.internal.JavaPropertyFinder.Visibility;

import org.junit.Test;

@SuppressWarnings("serial")
public class ForeignKeyHandlerTest {
	private JavaPropertyFinder privateFieldPublicBeanFinder = new JavaPropertyFinder(
			Visibility.PRIVATE, Visibility.PUBLIC, null, (PropertyType[])null);

	@Test
	public void isForeignGivenAnnotation() {
		ForeignKeyHandler handler = foreignKeyHandlerOf(Customer.class, "fkAddress");
		assertThat(handler.isForeignKey(), is(true));
	}

	@Test
	public void isnotForeignGivenAnnotation() {
		ForeignKeyHandler handler = foreignKeyHandlerOf(Customer.class, "customerUid");
		assertThat(handler.isForeignKey(), is(false));
	}
	
	@Test
	public void getsReferencedClassGivenValidAnnotation() {
		ForeignKeyHandler handler = foreignKeyHandlerOf(Customer.class, "fkAddress");
		assertThat(handler.getReferencedClass(), is((Object)Address.class));
	}

	@Test
	public void getsReferencedColumnGivenExplicitlySpecifiedOnAnnotation() {
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=Address.class, column="intValue")
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		ForeignKeyHandler handler = foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
		assertThat(handler.getReferencedColumnName(), is("intValue"));
	}

	@Test
	public void getsPrimaryKeyReferencedColumnGivenUnspecifiedOnAnnotation() {
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=Address.class)
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		ForeignKeyHandler handler = foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
		assertThat(handler.getReferencedColumnName(), is("addressUid"));
	}
	
	@Test
	public void getsReferenceeGivenCircularReferenceWithDepthZero() {
		@DBTableName("customer")
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=TestCustomer.class)
			@DBColumn
			public DBInteger fkPreviousHistory;
		}
		
		ForeignKeyHandler handler = foreignKeyHandlerOf(TestCustomer.class, "fkPreviousHistory");
		assertThat(handler.getReferencedTableName(), is("customer"));
		assertThat(handler.getReferencedColumnName(), is("customerUid"));
		assertThat(handler.getReferencedPropertyDefinitionIdentity(), is(not(nullValue())));
	}

	@DBTableName("address")
	class AddressWithCircularReferenceToCustomer extends DBRow {
		@DBPrimaryKey
		@DBColumn
		public DBInteger addressUid3;

		@DBColumn
		@DBForeignKey(value=CustomerWithCircularReferenceToAddress.class)
		public DBInteger fkCustomer;
	}

	@DBTableName("customer")
	class CustomerWithCircularReferenceToAddress extends DBRow {
		@DBPrimaryKey
		@DBColumn
		public DBInteger customerUid;
		
		@DBForeignKey(value=AddressWithCircularReferenceToCustomer.class)
		@DBColumn
		public DBInteger fkAddress3;
	}
	
	@Test
	public void getsReferenceeGivenCircularReferenceWithDepthOne() {
		ForeignKeyHandler handler = foreignKeyHandlerOf(CustomerWithCircularReferenceToAddress.class, "fkAddress3");
		assertThat(handler.getReferencedTableName(), is("address"));
		assertThat(handler.getReferencedColumnName(), is("addressUid3"));
		assertThat(handler.getReferencedPropertyDefinitionIdentity(), is(not(nullValue())));
	}
	
	@Test
	public void ignoresNonIdentityErrorsInReferencedClass() {
		class MyIntegerStringTypeAdaptor implements DBTypeAdaptor<Integer, String> {
			public Integer fromDatabaseValue(String dbvValue) {
				return null;
			}

			public String toDatabaseValue(Integer objectValue) {
				return null;
			}
		}
		
		class TestAddress extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger addressUid2;

			@DBColumn
			@DBForeignKey(value=Address.class, column="notAColumn")
			public DBInteger badFKColumn;
			
			@DBColumn
			@DBAdaptType(adaptor=MyIntegerStringTypeAdaptor.class, type=DBDate.class)
			public List<Object> date;
		}
		
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=TestAddress.class)
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		ForeignKeyHandler handler = foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
		assertThat(handler.getReferencedColumnName(), is("addressUid2"));
	}

	@Test
	public void getsReferencedTypeGivenValidTypeAdaptorAndNonIdentityErrorsInRestOfReferencedClass() {
		class MyStringIntegerTypeAdaptor implements DBTypeAdaptor<String, Integer> {
			public String fromDatabaseValue(Integer dbvValue) {
				return null;
			}

			public Integer toDatabaseValue(String objectValue) {
				return null;
			}
		}
		
		class TestAddress extends DBRow {
			@DBPrimaryKey
			@DBColumn
			@DBAdaptType(adaptor=MyStringIntegerTypeAdaptor.class)
			public String addressUid2;

			@DBColumn
			@DBForeignKey(value=Address.class, column="notAColumn")
			public DBInteger badFKColumn;
			
			@DBColumn
			@DBAdaptType(adaptor=MyStringIntegerTypeAdaptor.class, type=DBDate.class)
			public List<Object> date;

			@DBColumn
			public List<Object> needsTypeAdaptor;
			
			@DBColumn("oneName")
			public String getProperty1() {
				return null;
			}
			
			@DBColumn("differentName")
			public void setPropert1(String value) {
			}
		}
		
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=TestAddress.class)
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		ForeignKeyHandler handler = foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
		assertThat(handler.getReferencedPropertyDefinitionIdentity().type(), is((Object)DBInteger.class));
	}
	
	@Test(expected=DBPebkacException.class)
	public void errorsGivenReferencedColumnWithInvalidTypeAdaptor() {
		class MyStringDateTypeAdaptor implements DBTypeAdaptor<String, Date> {
			public String fromDatabaseValue(Date dbvValue) {
				return null;
			}

			public Date toDatabaseValue(String objectValue) {
				return null;
			}
		}
		
		class TestAddress extends DBRow {
			@DBPrimaryKey
			@DBColumn
			@DBAdaptType(adaptor=MyStringDateTypeAdaptor.class, type=DBInteger.class)
			public List<Object> badColumn;
		}
		
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=TestAddress.class)
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
	}
	
	@Test(expected=DBPebkacException.class)
	public void errorsWhenColumnExplicitlySpecifiedGivenIncorrectColumnName() {
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=Address.class, column="notAColumn")
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
	}

	@Test(expected=DBPebkacException.class)
	public void errorsWhenColumnExplicitlySpecifiedGivenDuplicateReferencedColumnName() {
		class TestAddress extends DBRow {
			@DBColumn
			public DBInteger addressUid;

			@DBColumn("addressUid")
			public DBInteger addressUid2;
			
			@DBColumn
			public DBInteger intValue;
		}
		
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=TestAddress.class, column="addressUid")
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
	}

	@Test(expected=DBPebkacException.class)
	public void errorsWhenColumnExplicitlySpecifiedGivenDuplicateButDifferingCaseReferencedColumnName() {
		class TestAddress extends DBRow {
			@DBColumn
			public DBInteger addressUid;

			@DBColumn("ADDRESSuid")
			public DBInteger addressUid2;
			
			@DBColumn
			public DBInteger intValue;
		}
		
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=TestAddress.class, column="addressUid")
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
	}
	
	@Test(expected=DBPebkacException.class)
	public void errorsGivenColumnUnspecifiedOnAnnotationAndNoPrimaryKey() {
		class TestAddress extends DBRow {
			@DBColumn
			public DBInteger addressUid;

			@DBColumn
			public DBInteger intValue;
		}
		
		class TestCustomer extends DBRow {
			@DBPrimaryKey
			@DBColumn
			public DBInteger customerUid;
			
			@DBForeignKey(value=TestAddress.class)
			@DBColumn
			public DBInteger fkAddress2;
		}
		
		foreignKeyHandlerOf(TestCustomer.class, "fkAddress2");
	}
	
	class Address extends DBRow {
		@DBPrimaryKey
		@DBColumn
		public DBInteger addressUid;

		@DBColumn
		public DBInteger intValue;
	}
	
	class Customer extends DBRow {
		@DBPrimaryKey
		@DBColumn
		public DBInteger customerUid;
		
		@DBForeignKey(value=Address.class)
		@DBColumn
		public DBInteger fkAddress;
	}
	
	private ForeignKeyHandler foreignKeyHandlerOf(Class<?> clazz, String javaPropertyName) {
		return new ForeignKeyHandler(propertyOf(clazz, javaPropertyName), false);
	}
	
	private JavaProperty propertyOf(Class<?> clazz, String javaPropertyName) {
		List<JavaProperty> properties = privateFieldPublicBeanFinder.getPropertiesOf(clazz);
		JavaProperty property = itemOf(properties, that(hasJavaPropertyName(javaPropertyName)));
		if (property == null) {
			throw new IllegalArgumentException("No property found with java name '"+javaPropertyName+"'");
		}
		return property;
	}
}
