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
import java.util.regex.Pattern;

/**
 * Simple access to creating a string of a variety of strings separated by a
 * common character or sequence.
 *
 * <p>
 * Also supports string prefix and suffix, and is a fluent API.</p>
 *
 * @author gregorygraham
 */
public class SeparatedString {

	private final String separator;
	private final ArrayList<String> strings = new ArrayList<>();
	private String prefix;
	private String suffix;
	private String wrapBefore = "";
	private String wrapAfter = "";
	private String escapeChar = "";

	public SeparatedString(String prefix, String separator, String suffix) {
		this.prefix = prefix;
		this.separator = separator;
		this.suffix = suffix;
	}

	public SeparatedString(String separator) {
		this("", separator, "");
	}
	
	public static SeparatedString bySpaces(){
		return new SeparatedString(" ");
	}

	public static SeparatedString byCommas(){
		return new SeparatedString(", ");
	}

	public static SeparatedString byCommasWithQuotedTermsAndBackslashEscape(){
		return new SeparatedString(", ").withWrapping("\"", "\"").withEscapeChar("\\");
	}
	
	public static SeparatedString byCommasWithQuotedTermsAndDoubleBackslashEscape(){
		return new SeparatedString(", ").withWrapping("\"", "\"").withEscapeChar("\\\\");
	}

	public static SeparatedString byTabs(){
		return new SeparatedString("\t");
	}

	public static SeparatedString byLines(){
		return new SeparatedString("\n");
	}
	
	public SeparatedString withWrapping(String before, String after){
		this.wrapBefore = before;
		this.wrapAfter = after;
		return this;
	}
	
	public SeparatedString withEscapeChar(String esc){
		this.escapeChar = esc;
		return this;
	}

	@Override
	public String toString() {
		StringBuilder strs = new StringBuilder();
		String sep = "";
		Pattern matchSep = Pattern.compile(getSeparator());
		Pattern matchBefore = Pattern.compile(getWrapBefore());
		Pattern matchAfter = Pattern.compile(getWrapAfter());
		Pattern matchEsc = Pattern.compile(getEscapeChar());
		for (String element : this.getStrings()) {
			String str = element;
			if(!escapeChar.equals("")){
				str = matchSep.matcher(str).replaceAll(getEscapeChar()+getSeparator());
				str = matchBefore.matcher(str).replaceAll(getEscapeChar()+getWrapBefore());
				str = matchAfter.matcher(str).replaceAll(getEscapeChar()+getWrapAfter());
				str = matchEsc.matcher(str).replaceAll(getEscapeChar()+getEscapeChar());
			}
			strs.append(sep).append(getWrapBefore()).append(str).append(getWrapAfter());
			sep = this.getSeparator();
		}
		return getPrefix() + strs.toString() + getSuffix();
	}

	public boolean isNotEmpty() {
		return !isEmpty();
	}

	public boolean isEmpty() {
		return getStrings().isEmpty();
	}

	public SeparatedString removeAll(Collection<?> c) {
		getStrings().removeAll(c);
		return this;
	}

	public SeparatedString addAll(int index, Collection<? extends String> c) {
		getStrings().addAll(index, c);
		return this;
	}

	public SeparatedString addAll(Collection<? extends String> c) {
		getStrings().addAll(c);
		return this;
	}

	public SeparatedString addAll(String... strs) {
		getStrings().addAll(Arrays.asList(strs));
		return this;
	}

	public SeparatedString remove(int index) {
		getStrings().remove(index);
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
	public SeparatedString add(int index, String element) {
		getStrings().add(index, element);
		return this;
	}

	public SeparatedString add(String string) {
		getStrings().add(string);
		return this;
	}

	/**
	 * @return the separator
	 */
	public String getSeparator() {
		return separator;
	}

	/**
	 * @return the strings
	 */
	public ArrayList<String> getStrings() {
		return strings;
	}

	/**
	 * @return the prefix
	 */
	public String getPrefix() {
		return prefix;
	}

	/**
	 * @return the suffix
	 */
	public String getSuffix() {
		return suffix;
	}

	/**
	 * @return the wrapBefore
	 */
	public String getWrapBefore() {
		return wrapBefore;
	}

	/**
	 * @return the wrapAfter
	 */
	public String getWrapAfter() {
		return wrapAfter;
	}

	/**
	 * @return the escapeChar
	 */
	public String getEscapeChar() {
		return escapeChar;
	}
}
