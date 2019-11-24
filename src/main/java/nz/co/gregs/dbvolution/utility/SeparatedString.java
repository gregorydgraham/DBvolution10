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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Simple access to creating a string of a variety of strings separated by a
 * common character or sequence.
 *
 * <p>
 * A common pattern is to add string elements to a longer string with a format
 * similar to: prefix|element1|separator|element2|suffix. For instance a file
 * path has prefix "/", separator "/", and suffix "". This class allows for
 * convenient object-oriented processing of this pattern.</p>
 *
 * <p>
 * Advanced features allow for proper CSV formatting including quoting and
 * escaping.</p>
 *
 * <p>
 * All values are strings, not characters, so complex output can be generated: a
 * WHEN clause in SQL would be
 * <p>
 * <code>SeparatedString.startsWith("WHEN").separatedBy(" AND ").addAll(allWhenClausesList).endsWith(groupByClauseString).toString();</code>
 *
 * <p>
 * The default separator is a space (" "). All other defaults are empty.</p>
 *
 * <p>
 * Supports string separator, prefix, suffix, quoting, before quote, after
 * quote, escaping, maps, and is a fluent API.</p>
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
	private String useWhenEmpty = "";
	private String keyValueSeparator = "";
	private boolean closedLoop = false;

	private SeparatedString() {
	}

	public static SeparatedString startsWith(String precedingString) {
		return new SeparatedString().withPrefix(precedingString);
	}

	public static SeparatedString of(Map<String, String> nameValuePairs, String nameValueSeparator) {
		ArrayList<String> list = new ArrayList<>();
		nameValuePairs.entrySet().forEach((entry) -> {
			String key = entry.getKey();
			String val = entry.getValue();
			list.add(key + nameValueSeparator + val);
		});
		if (list != null) {
			return new SeparatedString().addAll(list);
		} else {
			return new SeparatedString();
		}
	}

	public static SeparatedString of(String... allStrings) {
		return new SeparatedString().addAll(allStrings);
	}

	public static SeparatedString of(List<String> allStrings) {
		final SeparatedString separatedString = new SeparatedString();
		if (allStrings != null) {
			final String[] toArray = allStrings.toArray(new String[]{});
			return separatedString.addAll(toArray);
		}
		return separatedString;
	}

	public static SeparatedString forSeparator(String separator) {
		return new SeparatedString().separatedBy(separator);
	}

	public SeparatedString separatedBy(String separator) {
		if (separator != null && !separator.isEmpty()) {
			this.separator = separator;
		}
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

	public SeparatedString withKeyValueSeparator(String probablyEquals) {
		this.keyValueSeparator = probablyEquals;
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
			String currentElement = "";
			String firstElement = null;
			for (String element : allTheElements) {
				String str = element;
				if (!escapeChar.equals("")) {
					str = matchBefore.matcher(str).replaceAll(getEscapeChar() + getWrapBefore());
					str = matchAfter.matcher(str).replaceAll(getEscapeChar() + getWrapAfter());
					str = matchEsc.matcher(str).replaceAll(getEscapeChar() + getEscapeChar());
				}
				currentElement = getWrapBefore() + str + getWrapAfter();
				if (firstElement == null) {
					firstElement = currentElement;
				}
				strs.append(sep).append(currentElement);
				sep = this.getSeparator();
			}
			if (this.closedLoop && firstElement != null && !firstElement.equals(currentElement)) {
				strs.append(sep).append(firstElement);
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

	public SeparatedString addAll(int index, Collection<String> c) {
		if (c != null) {
			getStrings().addAll(index, c);
		}
		return this;
	}

	public SeparatedString addAll(Collection<String> c) {
		if (c != null && !c.isEmpty()) {
			getStrings().addAll(c);
		}
		return this;
	}

	public SeparatedString addAll(Map<String, String> c) {
		if (c != null && !c.isEmpty()) {
			c.forEach((key, value) -> {
				getStrings().add(key + getKeyValueSeparator() + value);
			});
		}
		return this;
	}

	public SeparatedString addAll(Map<String, String> c, String keyValueSeparator) {
		withKeyValueSeparator(keyValueSeparator);
		addAll(c);
		return this;
	}

	public SeparatedString addAll(String... strs) {
		final List<String> asList = Arrays.asList(strs);
		if (asList != null) {
			getStrings().addAll(asList);
		}
		return this;
	}

	public SeparatedString addAll(Function<String, String> stringProcessor, String... strs) {
		return addAll(stringProcessor, Arrays.asList(strs));
	}

	public SeparatedString addAll(Function<String, String> stringProcessor, List<String> strs) {
		final List<String> asList = strs.stream().map(stringProcessor).collect(Collectors.toList());
		if (asList != null) {
			getStrings().addAll(asList);
		}
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

	public SeparatedString containing(String... strings) {
		return addAll(strings);
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

	public SeparatedString withClosedLoop() {
		this.closedLoop = true;
		return this;
	}

	public SeparatedString withoutClosedLoop() {
		this.closedLoop = false;
		return this;
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

	public List<String> parseToList(String input) {
		List<String> list = new ArrayList<>(0);
		if (input != null) {
			String str = input;
			if (hasValue(getWrapBefore())) {
				str = str.replaceFirst("^" + getWrapBefore(), "");
			}
			if (hasValue(getWrapAfter())) {
				str = str.replaceAll(getWrapAfter() + "$", "");
			}
			if (str != null && !str.isEmpty()) {
				String[] split = str.split(getSeparator());
				final List<String> asList = Arrays.asList(split);
				if (asList != null) {
					list.addAll(asList);
				}
			}
		}
		return list;
	}

	public Map<String, String> parseToMap(String input) {
		Map<String, String> map = new HashMap<>(0);
		if (input == null || input.isEmpty()) {
			return map;
		}
		String[] split = input.split(getSeparator());
		for (String string : split) {
			String[] split2 = string.split(getKeyValueSeparator());
			if (split2.length == 2) {
				map.put(split2[0], split[1]);
			} else if (split2.length == 1) {
				map.put(split2[0], "");
			}
		}
		return map;
	}

	public String getKeyValueSeparator() {
		return keyValueSeparator;
	}

	private boolean hasValue(String value) {
		return value != null && !value.isEmpty();
	}
}
