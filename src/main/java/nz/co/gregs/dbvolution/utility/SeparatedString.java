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
 * The default separator is a space (" "). All other defaults are empty.</p>
 *
 * <p>
 * Also supports string prefix and suffix, and is a fluent API.</p>
 *
 * @author gregorygraham
 */
public class SeparatedString {

	private String separator = " ";
	private final ArrayList<String> strings = new ArrayList<>();
	private String prefix = "";
	private String suffix = "";
	private String wrapBefore = "";
	private String wrapAfter = "";
	private String escapeChar = "";
	private String useWhenEmpty;

	private SeparatedString() {
	}

	public static SeparatedString startsWith(String precedingString) {
		return new SeparatedString().withPrefix(precedingString);
	}

	public static SeparatedString of(String... allStrings) {
		return new SeparatedString().add(allStrings);
	}

	public static SeparatedString of(Object... allStrings) {
		return new SeparatedString().add(allStrings);
	}

	public static SeparatedString forSeparator(String separator) {
		return new SeparatedString().separatedBy(separator);
	}

	public SeparatedString separatedBy(String separator) {
		this.separator = separator;
		return this;
	}

	public static SeparatedString bySpaces() {
		return SeparatedString.forSeparator(" ");
	}

	public static SeparatedString byCommas() {
		return SeparatedString.forSeparator(", ");
	}

	public static SeparatedString byCommasWithQuotedTermsAndBackslashEscape() {
		return SeparatedString
				.forSeparator(", ")
				.withWrapBefore("\"")
				.withWrapAfter("\"")
				.withEscapeChar("\\");
	}

	public static SeparatedString byCommasWithQuotedTermsAndDoubleBackslashEscape() {
		return SeparatedString
				.forSeparator(", ")
				.withWrapBefore("\"")
				.withWrapAfter("\"")
				.withEscapeChar("\\\\");
	}

	public static SeparatedString byTabs() {
		return SeparatedString.forSeparator("\t");
	}

	public static SeparatedString byLines() {
		return SeparatedString.forSeparator("\n");
	}

	public static SeparatedString spaceSeparated() {
		return SeparatedString.bySpaces();
	}

	public static SeparatedString commaSeparated() {
		return SeparatedString.byCommas();
	}

	public static SeparatedString tabSeparated() {
		return SeparatedString.byTabs();
	}

	public static SeparatedString lineSeparated() {
		return SeparatedString.byLines();
	}

	public SeparatedString withEscapeChar(String esc) {
		this.escapeChar = esc;
		return this;
	}

	@Override
	public String toString() {
		final ArrayList<String> allTheElements = getStrings();
		if (allTheElements.isEmpty()) {
			return useWhenEmpty;
		} else {
			StringBuilder strs = new StringBuilder();
			String sep = "";
			Pattern matchBefore = Pattern.compile(getWrapBefore());
			Pattern matchAfter = Pattern.compile(getWrapAfter());
			Pattern matchEsc = Pattern.compile(getEscapeChar());
			for (String element : allTheElements) {
				String str = element;
				if (!escapeChar.equals("")) {
					str = matchBefore.matcher(str).replaceAll(getEscapeChar() + getWrapBefore());
					str = matchAfter.matcher(str).replaceAll(getEscapeChar() + getWrapAfter());
					str = matchEsc.matcher(str).replaceAll(getEscapeChar() + getEscapeChar());
				}
				strs.append(sep).append(getWrapBefore()).append(str).append(getWrapAfter());
				sep = this.getSeparator();
			}
			return getPrefix() + strs.toString() + getSuffix();
		}
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

	public SeparatedString add(Object string) {
		getStrings().add(string.toString());
		return this;
	}

	public SeparatedString add(Object... strings) {
		Arrays.asList(strings).stream().forEach((t) -> add(t));
		return this;
	}

	public SeparatedString add(String... strings) {
		getStrings().addAll(Arrays.asList(strings));
		return this;
	}

	public SeparatedString containing(String... strings) {
		return add(strings);
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

	public SeparatedString withWrapper(String wrapAroundEachTerm) {
		this.wrapBefore = wrapAroundEachTerm;
		this.wrapAfter = wrapAroundEachTerm;
		return this;
	}

	public SeparatedString withWrapBefore(String wrapAtTheBeginningOfTerms) {
		this.wrapBefore = wrapAtTheBeginningOfTerms;
		return this;
	}

	public SeparatedString withWrapAfter(String placeAtTheEndOfEachTerm) {
		this.wrapAfter = placeAtTheEndOfEachTerm;
		return this;
	}

	public SeparatedString withPrefix(String placeAtTheBeginningOfTheString) {
		this.prefix = placeAtTheBeginningOfTheString;
		return this;
	}

	public SeparatedString withSuffix(String placeAtTheEndOfTheString) {
		this.suffix = placeAtTheEndOfTheString;
		return this;
	}

	public final SeparatedString endsWith(String string) {
		return withSuffix(string);
	}

	public final SeparatedString useWhenEmpty(String string) {
		this.useWhenEmpty = string;
		return this;
	}
}
