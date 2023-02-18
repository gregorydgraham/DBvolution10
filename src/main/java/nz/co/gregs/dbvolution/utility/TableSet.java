/*
 * Copyright 2023 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.utility;

import java.util.Collection;
import nz.co.gregs.dbvolution.DBRow;

/**
 *
 * @author gregorygraham
 */
public class TableSet extends java.util.HashSet<DBRow> {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructs a new, empty set; the backing {@code HashMap} instance has
	 * default initial capacity (16) and load factor (0.75).
	 */
	public TableSet() {
		super(0);
	}

	/**
	 * Constructs a new set containing the elements in the specified collection.
	 * The {@code HashMap} is created with default load factor (0.75) and an
	 * initial capacity sufficient to contain the elements in the specified
	 * collection.
	 *
	 * @param c the collection whose elements are to be placed into this set
	 * @throws NullPointerException if the specified collection is null
	 */
	public TableSet(Collection<DBRow> c) {
		super(c);
	}

	/**
	 * Constructs a new, empty set; the backing {@code HashMap} instance has the
	 * specified initial capacity and the specified load factor.
	 *
	 * @param initialCapacity the initial capacity of the hash map
	 * @param loadFactor the load factor of the hash map
	 * @throws IllegalArgumentException if the initial capacity is less than zero,
	 * or if the load factor is nonpositive
	 */
	public TableSet(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	@Override
	public boolean contains(Object o) {
		if (o == null) {
			return true;
		}
		if (o instanceof DBRow) {
			DBRow row = (DBRow) o;
			String tableName = row.getTableName();
			for (Object object : this.toArray()) {
				if (object instanceof DBRow) {
					DBRow oldRow = (DBRow) object;
					return tableName.equals(oldRow.getTableName());
				}
			}
		}
		return false;
	}
}
