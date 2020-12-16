/*
 * Copyright 2020 Gregory Graham.
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
import java.util.List;

/**
 *
 * @author gregorygraham
 */
public abstract class RegexGroup<THIS extends RegexGroup<THIS>> implements HasRegexFunctions<THIS> {

	private final Regex origin;
	private Regex current = Regex.startingAnywhere();

	public RegexGroup(Regex original) {
		this.origin = original;
	}

	/**
	 * @return the current
	 */
	public Regex getCurrent() {
		return current;
	}

	/**
	 * @return the origin
	 */
	public Regex getOrigin() {
		return origin;
	}

	public Regex closeGroup() {
		return getOrigin().unescaped(this.getRegexp());
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS notPrecededBy(String literalValue) {
		current = getCurrent().notPrecededBy(literalValue);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS noneOf(String literals) {
		current = getCurrent().noneOf(literals);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS nothingBetween(Character lowest, Character highest) {
		current = getCurrent().nothingBetween(lowest, highest);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS anyOf(String literals) {
		current = getCurrent().anyOf(literals);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS anyBetween(Character lowest, Character highest) {
		current = getCurrent().anyBetween(lowest, highest);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS number() {
		current = getCurrent().number();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS integer() {
		current = getCurrent().integer();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS positiveInteger() {
		current = getCurrent().positiveInteger();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS negativeInteger() {
		current = getCurrent().negativeInteger();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS capture(Regex regexp) {
		current = getCurrent().capture(regexp);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS space() {
		current = getCurrent().space();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS nonWhitespace() {
		current = getCurrent().nonWhitespace();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS whitespace() {
		current = getCurrent().whitespace();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS nonWordCharacter() {
		current = getCurrent().nonWordCharacter();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS gapBetweenWords() {
		current = getCurrent().gapBetweenWords();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS word() {
		current = getCurrent().word();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS wordCharacter() {
		current = getCurrent().wordCharacter();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS nondigits() {
		current = getCurrent().nondigits();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS nondigit() {
		current = getCurrent().nondigit();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS digits() {
		current = getCurrent().digits();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS digit() {
		current = getCurrent().digit();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS endOfTheString() {
		current = getCurrent().endOfTheString();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS optionalMany() {
		current = getCurrent().optionalMany();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS zeroOrMore() {
		current = getCurrent().zeroOrMore();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS oneOrMore() {
		current = getCurrent().oneOrMore();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS atLeastOnce() {
		current = getCurrent().atLeastOnce();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS onceOrNotAtAll() {
		current = getCurrent().onceOrNotAtAll();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS atLeastXAndNoMoreThanYTimes(int x, int y) {
		current = getCurrent().atLeastXAndNoMoreThanYTimes(x, y);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS atLeastThisManyTimes(int x) {
		current = getCurrent().atLeastThisManyTimes(x);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS thisManyTimes(int x) {
		current = getCurrent().thisManyTimes(x);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS once() {
		current = getCurrent().once();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS anyCharacter() {
		current = getCurrent().anyCharacter();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS controlCharacter(String x) {
		current = getCurrent().controlCharacter(x);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS escapeCharacter() {
		current = getCurrent().escapeCharacter();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS bell() {
		current = getCurrent().bell();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS formfeed() {
		current = getCurrent().formfeed();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS carriageReturn() {
		current = getCurrent().carriageReturn();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS newline() {
		current = getCurrent().newline();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS tab() {
		current = getCurrent().tab();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS bracket() {
		current = getCurrent().bracket();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS squareBracket() {
		current = getCurrent().squareBracket();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS pipe() {
		current = getCurrent().pipe();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS asterisk() {
		current = getCurrent().asterisk();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS star() {
		current = getCurrent().star();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS plus() {
		current = getCurrent().plus();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS questionMark() {
		current = getCurrent().questionMark();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS dot() {
		current = getCurrent().dot();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS dollarSign() {
		current = getCurrent().dollarSign();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS carat() {
		current = getCurrent().carat();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS backslash() {
		current = getCurrent().backslash();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS literal(String literals) {
		current = getCurrent().literal(literals);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS literal(Character character) {
		current = getCurrent().literal(character);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS extend(HasRegexFunctions<?> second) {
		current = getCurrent().extend(second);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS add(HasRegexFunctions<?> second) {
		current = getCurrent().add(second);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS wordBoundary() {
		current = getCurrent().wordBoundary();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS nonWordBoundary() {
		current = getCurrent().nonWordBoundary();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS theBeginningOfTheInput() {
		current = getCurrent().theBeginningOfTheInput();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS theEndOfThePreviousMatch() {
		current = getCurrent().theEndOfThePreviousMatch();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS theEndOfTheInput() {
		current = getCurrent().theEndOfTheInput();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS theEndOfTheInputButForTheFinalTerminator() {
		current = getCurrent().theEndOfTheInputButForTheFinalTerminator();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS notPrecededBy(Regex literalValue) {
		current = getCurrent().notPrecededBy(literalValue);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS notFollowedBy(String literalValue) {
		current = getCurrent().notFollowedBy(literalValue);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS notFollowedBy(Regex literalValue) {
		current = getCurrent().notFollowedBy(literalValue);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS addGroup(HasRegexFunctions<?> second) {
		current = getCurrent().addGroup(second);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS groupEverythingBeforeThis() {
		current = getCurrent().groupEverythingBeforeThis();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS numberLike() {
		current = getCurrent().numberLike();
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS literalCaseInsensitive(String literals) {
		current = getCurrent().literalCaseInsensitive(literals);
		return (THIS) this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public THIS unescaped(String unescapedSequence) {
		current = getCurrent().unescaped(unescapedSequence);
		return (THIS) this;
	}

	@SuppressWarnings("unchecked")
	public Regex.RangeBuilder<THIS> openRange(char lowest, char highest) {
		return new Regex.RangeBuilder<THIS>((THIS) this, lowest, highest);
	}

	@SuppressWarnings("unchecked")
	public Regex.RangeBuilder<THIS> openRange(String literals) {
		return new Regex.RangeBuilder<THIS>((THIS) this, literals);
	}

	public static class Or extends RegexGroup<Or> {

		private final List<String> ors = new ArrayList<>(0);

		protected Or(Regex original) {
			super(original);
		}

		protected Or(Regex original, List<String> previousOptions) {
			super(original);
			ors.addAll(previousOptions);
		}

		public Or or() {
			ors.add(getCurrent().getRegexp());
			return new Or(getOrigin(), ors);
		}

		@Override
		public String getRegexp() {
			final String regexp = getCurrent().getRegexp();
			ors.add(regexp);
			final SeparatedString groupedString = SeparatedString.of(ors).separatedBy("|").withPrefix("(").withSuffix(")");
			return groupedString.toString();
		}
	}

	public static class CaseInsensitive extends RegexGroup<CaseInsensitive> {

		public CaseInsensitive(Regex original) {
			super(original);
		}

		@Override
		public String getRegexp() {
			final String regexp = getCurrent().getRegexp();
			return "(?i)" + regexp + "(?-i)";
		}

		public Regex caseInsensitiveEnd() {
			return closeGroup();
		}
	}

}
