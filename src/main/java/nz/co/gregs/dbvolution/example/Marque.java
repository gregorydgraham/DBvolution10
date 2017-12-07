package nz.co.gregs.dbvolution.example;

import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import java.util.Date;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;
import nz.co.gregs.dbvolution.datatypes.DBStringTrimmed;

/**
 * A DBRow Java class that represents the "marque" table.
 *
 * <p>
 * &#64;DBTableName annotation allows the class to be renamed to fit better
 * within a Java library while preserving the actual database name.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
@DBTableName("marque")
public class Marque extends DBRow {

	private static final long serialVersionUID = 1L;

	/**
	 * A DBNumber field representing the "numeric_code" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBNumber indicates that the field is INTEGER or NUMBER field that naturally
	 * provides Number values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("numeric_code")
	public DBNumber numericCode = new DBNumber();

	/**
	 * A DBInteger field representing the "uid_marque" column in the database.
	 *
	 * <p>
	 * &#64;DBPrimaryKey both indicates that the field is the primary key of the
	 * table and should be used to connect other related tables to this table.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 * DBInteger is the usual datatype of database primary keys.
	 *
	 * <p>
	 * DBInteger indicates that the field is INTEGER or NUMBER field that
	 * naturally provides Number values in Java. It has an instance as that just
	 * makes everyone's life easier.
	 *
	 */
	@DBColumn("uid_marque")
	@DBPrimaryKey
	public DBInteger uidMarque = new DBInteger();

	/**
	 * A DBString field representing the "isusedfortafros" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBString indicates that the field is CHAR or VARCHAR field that naturally
	 * provides String values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("isusedfortafros")
	public DBString isUsedForTAFROs = new DBString();

	/**
	 * A DBNumber field representing the "fk_toystatusclass" column in the
	 * database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBNumber indicates that the field is INTEGER or NUMBER field that naturally
	 * provides Number values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("fk_toystatusclass")
	public DBNumber statusClassID = new DBNumber();

	/**
	 * A DBString field representing the "intindallocallowed" column in the
	 * database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBString indicates that the field is CHAR or VARCHAR field that naturally
	 * provides String values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("intindallocallowed")
	public DBString individualAllocationsAllowed = new DBString();

	/**
	 * A DBInteger field representing the "upd_count" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 * DBInteger is the usual datatype of database primary keys.
	 *
	 * <p>
	 * DBInteger indicates that the field is INTEGER or NUMBER field that
	 * naturally provides Number values in Java. It has an instance as that just
	 * makes everyone's life easier.
	 *
	 */
	@DBColumn("upd_count")
	public DBInteger updateCount = new DBInteger();

	/**
	 * A DBString field representing the "auto_created" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBString indicates that the field is CHAR or VARCHAR field that naturally
	 * provides String values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn
	public DBStringTrimmed auto_created = new DBStringTrimmed();

	/**
	 * A DBString field representing the "name" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBString indicates that the field is CHAR or VARCHAR field that naturally
	 * provides String values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn
	public DBString name = new DBString();

	/**
	 * A DBString field representing the "pricingcodeprefix" column in the
	 * database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBString indicates that the field is CHAR or VARCHAR field that naturally
	 * provides String values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("pricingcodeprefix")
	public DBString pricingCodePrefix = new DBString();

	/**
	 * A DBString field representing the "reservationsalwd" column in the
	 * database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBString indicates that the field is CHAR or VARCHAR field that naturally
	 * provides String values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("reservationsalwd")
	public DBString reservationsAllowed = new DBString();

	/**
	 * A DBDate field representing the "creation_date" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBDate indicates that the field is a DATE or TIMESTAMP field that naturally
	 * provides Date values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("creation_date")
	public DBDate creationDate = new DBDate();

	/**
	 * A DBBoolean field representing the "enabled" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBBoolean indicates that the field is BOOLEAN or BIT field that naturally
	 * provides Boolean values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("enabled")
	public DBBoolean enabled = new DBBoolean();

	/**
	 * A DBInteger field representing the "fk_carcompany" column in the database.
	 *
	 * <p>
	 * &#64;DBForeignKey indicates that this field is a reference to the primary
	 * key of the table represented by CarCompany.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBInteger indicates that the field is INTEGER or NUMBER field that
	 * naturally provides Integer values in Java. It has an instance as that just
	 * makes everyone's life easier.
	 *
	 */
	@DBForeignKey(CarCompany.class)
	@DBColumn("fk_carcompany")
	public DBInteger carCompany = new DBInteger();

	/**
	 * Required Public No-Argument Constructor.
	 *
	 */
	public Marque() {
	}

	/**
	 * Convenience Constructor.
	 *
	 * @param uidMarque uidMarque
	 * @param isUsedForTAFROs isUsedForTAFROs
	 * @param statusClass statusClass
	 * @param carCompany carCompany
	 * @param intIndividualAllocationsAllowed intIndividualAllocationsAllowed
	 * @param pricingCodePrefix pricingCodePrefix
	 * @param updateCount updateCount
	 * @param name name
	 * @param reservationsAllowed reservationsAllowed
	 * @param autoCreated autoCreated
	 * @param creationDate creationDate
	 * @param enabled enabled
	 */
	public Marque(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, Integer updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, Date creationDate, int carCompany, Boolean enabled) {
		this.uidMarque.setValue(uidMarque);
		this.isUsedForTAFROs.setValue(isUsedForTAFROs);
		this.statusClassID.setValue(statusClass);
		this.individualAllocationsAllowed.setValue(intIndividualAllocationsAllowed);
		this.updateCount.setValue(updateCount);
		this.auto_created.setValue(autoCreated);
		this.name.setValue(name);
		this.pricingCodePrefix.setValue(pricingCodePrefix);
		this.reservationsAllowed.setValue(reservationsAllowed);
		this.creationDate.setValue(creationDate);
		this.carCompany.setValue(carCompany);
		this.enabled.setValue(enabled);
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the numericCode
	 */
	public DBNumber getNumericCode() {
		return numericCode;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the uidMarque
	 */
	public DBInteger getUidMarque() {
		return uidMarque;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the isUsedForTAFROs
	 */
	public DBString getIsUsedForTAFROs() {
		return isUsedForTAFROs;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the intIndividualAllocationsAllowed
	 */
	public DBString getIntIndividualAllocationsAllowed() {
		return individualAllocationsAllowed;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the name
	 */
	public DBString getName() {
		return name;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the creationDate
	 */
	public DBDate getCreationDate() {
		return creationDate;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the carCompany
	 */
	public DBInteger getCarCompany() {
		return carCompany;
	}
}
