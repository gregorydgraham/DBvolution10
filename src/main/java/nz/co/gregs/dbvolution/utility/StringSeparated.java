/*
 * Copyright 2019 Gregory Graham.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 *
 * @author gregorygraham
 */
public class StringSeparated {

	private final String separator;
	private final ArrayList<String> strings = new ArrayList<>();
	private String prefix;
	private String suffix;
	
	public StringSeparated(String prefix, String separator, String suffix) {
		this.prefix =prefix;
		this.separator = separator;
		this.suffix = suffix;
	}

	public StringSeparated(String separator) {
		this("", separator, "");
	}

	@Override
	public String toString() {
		StringBuilder strs = new StringBuilder();
		String sep = "";
		for (String str : this.strings) {
			strs.append(sep).append(str);
			sep = this.separator;
		}
		return prefix+strs.toString()+suffix;
	}

	public boolean isNotEmpty() {
		return !isEmpty();
	}

	public boolean isEmpty() {
		return strings.isEmpty();
	}

	public StringSeparated removeAll(Collection<?> c) {
		strings.removeAll(c);
		return this;
	}

	public StringSeparated addAll(int index, Collection<? extends String> c) {
		strings.addAll(index, c);
		return this;
	}

	public StringSeparated addAll(Collection<? extends String> c) {
		strings.addAll(c);
		return this;
	}

	public StringSeparated addAll(String... strs) {
		strings.addAll(Arrays.asList(strs));
		return this;
	}

	public StringSeparated remove(int index) {
		strings.remove(index);
		return this;
	}

	/**
	 * Inserts the specified element at the specified position in this list.Shifts
	 * the element currently at that position (if any) and any subsequent elements
	 * to the right (adds one to their indices).
	 *
	 * @param index index at which the specified element is to be inserted
	 * @param element element to be inserted
	 * @return this
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public StringSeparated add(int index, String element) {
		strings.add(index, element);
		return this;
	}

	public StringSeparated add(String safeString) {
		strings.add(safeString);
		return this;
	}
}
