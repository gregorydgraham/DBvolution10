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
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author gregorygraham
 */
public abstract class Regex implements HasRegexFunctions<Regex> {

	private Pattern compiledVersion;

	private Regex() {
	}

	public abstract String getRegexp();

	/**
	 * Create a new empty regular expression.
	 *
	 * @return a new empty regular expression
	 */
	public static Regex startingAnywhere() {
		return new UnescapedSequence("");
	}

	/**
	 * Create a new regular expression that includes a test for the start of the
	 * string.
	 *
	 * @return a new regular expression
	 */
	public static Regex startingFromTheBeginning() {
		return new UnescapedSequence("^");
	}

	/**
	 * Adds the regular expression to the end of current expression as a new
	 * group.
	 *
	 * <p>
	 * For example Regex.startingAnywhere().add(allowedValue).add(separator) will
	 * add the "separator" regular expression to the "allowedValue" expression
	 * (the rest of the instruction adds nothing). Assuming that allowedValue is
	 * "[0-9]" and separator is ":", the full regexp will be "([0-9])(:)".
	 *
	 * @param second
	 * @return a new regular expression consisting of the current expression and
	 * the supplied expression added together
	 */
	@Override
	public Regex add(Regex second) {
		return new RegexpCombination(this, second.groupEverythingBeforeThis());
	}

	/**
	 * Adds the regular expression to the end of current expression without
	 * grouping it.
	 *
	 * <p>
	 * Not grouping the added regular expression can produce counter-intuitive
	 * results and breaks encapsulation so use it carefully. In Particular
	 * extend(myRegex).onceOrNotAtAll() will only apply the "onceOrNotAtAll" to
	 * last element of myRegex and not the entire expression. Using
	 * digit.extend(Regex.startAnywhere().dot().digits()).onceOrNotAtAll() will
	 * match "0." and "0.5" but not "0". If you want grouping use add() instead.
	 *
	 * <p>
	 * For example Regex.startingAnywhere().extend(allowedValue).extend(separator)
	 * will add the "separator" regular expression to the "allowedValue"
	 * expression (the rest of the instruction adds nothing). Assuming that
	 * allowedValue is "[0-9]" and separator is ":", the full regexp will be
	 * "[0-9]:".
	 *
	 * @param second
	 * @return a new regular expression consisting of the current expression and
	 * the supplied expression added together
	 */
	@Override
	public Regex extend(Regex second) {
		return new RegexpCombination(this, second);
	}

	@Override
	public Regex literal(Character character) {
		if (character.equals('/')) {
			return extend(backslash());
		} else {
			return extend(new SingleCharacter(character));
		}
	}

	/**
	 * Adds a literal string to the regexp without grouping it.
	 *
	 *
	 * @param literals
	 * @return a new regexp
	 */
	@Override
	public Regex literal(String literals) {
		return extend(new LiteralSequence(literals));
	}

	/**
	 * Adds a unescaped sequence to the regexp without grouping it.
	 *
	 *
	 * @param literals
	 * @return a new regexp
	 */
	@Override
	public Regex unescaped(String literals) {
		return extend(new UnescapedSequence(literals));
	}

	/**
	 * Adds a literal backslash(\) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex backslash() {
		return extend(new UnescapedSequence("\\\\"));
	}

	/**
	 * Adds a literal carat (^) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex carat() {
		return extend(new UnescapedSequence("\\^"));
	}

	/**
	 * Adds a literal dollar sign($) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex dollarSign() {
		return extend(new UnescapedSequence("\\$"));
	}

	/**
	 * Adds a literal dot(.) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex dot() {
		return extend(new UnescapedSequence("\\."));
	}

	/**
	 * Adds a literal question mark(?) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex questionMark() {
		return extend(new UnescapedSequence("\\?"));
	}

	/**
	 * Adds a literal plus sign(+) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex plus() {
		return extend(new UnescapedSequence("\\+"));
	}

	/**
	 * Adds a literal star(*) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex star() {
		return extend(new UnescapedSequence("\\*"));
	}

	/**
	 * Adds a literal asterisk(*) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex asterisk() {
		return extend(new UnescapedSequence("\\*"));
	}

	/**
	 * Adds a literal pipe(|) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex pipe() {
		return extend(new UnescapedSequence("\\|"));
	}

	/**
	 * Adds a literal square bracket([) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex squareBracket() {
		return extend(new UnescapedSequence("\\["));
	}

	/**
	 * Adds a literal bracket, i.e. "(", to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex bracket() {
		return extend(new UnescapedSequence("\\("));
	}

	/**
	 * Adds a tab character(\t) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex tab() {
		return extend(new UnescapedSequence("\\t"));
	}

	/**
	 * Adds a newline character(\n) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex newline() {
		return extend(new UnescapedSequence("\\n"));
	}

	/**
	 * Adds a carriage return character(\r) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex carriageReturn() {
		return extend(new UnescapedSequence("\\r"));
	}

	/**
	 * Adds a formfeed character(\f) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex formfeed() {
		return extend(new UnescapedSequence("\\f"));
	}

	/**
	 * Adds a bell character(\a) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex bell() {
		return extend(new UnescapedSequence("\\a"));
	}

	/**
	 * Adds a escape character(\e) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex escapeCharacter() {
		return extend(new UnescapedSequence("\\e"));
	}

	/**
	 * Adds a control character(\cX) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex controlCharacter(String x) {
		return extend(new UnescapedSequence("\\c" + x));
	}

	/**
	 * Adds a match for any single character to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex anyCharacter() {
		return extend(new UnescapedSequence("."));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex once() {
		return extend(new UnescapedSequence("{1}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly X number of times.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex thisManyTimes(int x) {
		return extend(new UnescapedSequence("{" + x + "}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position X times or more.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex atLeastThisManyTimes(int x) {
		return extend(new UnescapedSequence("{" + x + ",}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position X or more times but no more than Y times.
	 *
	 * <p>
	 * literal('a').atLeastXAndNoMoreThanYTimes(2,3) will match "aa" and "aaa" but
	 * not "aa" nor "aaaa".
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex atLeastXAndNoMoreThanYTimes(int x, int y) {
		return extend(new UnescapedSequence("{" + x + "," + y + "}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once or not at all.
	 *
	 * <p>
	 * literal('a').literal('b)'.onceOrNotAtAll() will match "a" or "ab", but not
	 * "abb"
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex onceOrNotAtAll() {
		return extend(new UnescapedSequence("?"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once or not at all.
	 *
	 * <p>
	 * literal('a').literal('b)'.atLeastOnce() will match "ab" or "abb", but not
	 * "a"
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex atLeastOnce() {
		return extend(new UnescapedSequence("+"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once or not at all.
	 *
	 * <p>
	 * literal('a').literal('b)'.atLeastOnce() will match "ab" or "abb", but not
	 * "a"
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex oneOrMore() {
		return atLeastOnce();
	}

	/**
	 * Alters the previous element in the regexp so that it matches if the element
	 * appears in that position or not.
	 *
	 * <p>
	 * literal('a').literal('b)'.zeroOrMore().literal('c') will match "ac" or
	 * "abc".
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex zeroOrMore() {
		return extend(new UnescapedSequence("*"));
	}

	/**
	 * Alters the previous element in the regexp so that it matches if the element
	 * appears in that position or not.
	 *
	 * <p>
	 * literal('a').literal('b)'.zeroOrMore().literal('c') will match "ac" or
	 * "abc".
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex optionalMany() {
		return zeroOrMore();
	}

	/**
	 * Adds a check for the end of the string to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex endOfTheString() {
		return extend(new UnescapedSequence("$"));
	}

	/**
	 * Adds a check for a digit(0123456789) to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex digit() {
		return extend(new UnescapedSequence("\\d"));
	}

	/**
	 * Adds a check for one or more digits to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * Please note that digits is not the same as a valid integer or number, use {@link #positiveInteger() }, {@link #negativeInteger() }, {@link #integer()
	 * }, or {@link #number()} instead.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex digits() {
		return digit().oneOrMore();
	}

	/**
	 * Adds a check for anything other than a digit(0123456789) to the regular
	 * expression without grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex nondigit() {
		return extend(new UnescapedSequence("\\D"));
	}

	/**
	 * Adds a check for one or more of anything other than a digit(0123456789) to
	 * the regular expression without grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex nondigits() {
		return nondigit().oneOrMore();
	}

	/**
	 * Adds a check for a word character(\w) to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * A word character is any letter A-Z, upper or lowercase, any digit, or the
	 * underscore character.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex wordCharacter() {
		return extend(new UnescapedSequence("\\w"));
	}

	/**
	 * Adds a check for one or more word characters(\w) to the regular expression
	 * without grouping.
	 *
	 * <p>
	 * A word character is any letter A-Z, upper or lowercase, any digit, or the
	 * underscore character.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex word() {
		return wordCharacter().oneOrMore();
	}

	/**
	 * Adds a check for one or more non-word characters(\w) to the regular
	 * expression without grouping.
	 *
	 * <p>
	 * A word character is any letter A-Z, upper or lowercase, any digit, or the
	 * underscore character. A non-word character is any other character.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex gapBetweenWords() {
		return nonWordCharacter().oneOrMore();
	}

	/**
	 * Adds a check for a non-word character(\w) to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * A word character is any letter A-Z, upper or lowercase, any digit, or the
	 * underscore character. A non-word character is any other character.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex nonWordCharacter() {
		return extend(new UnescapedSequence("\\W"));
	}

	/**
	 * Adds a check for a whitespace character(\w) to the regular expression
	 * without grouping.
	 *
	 * <p>
	 * A whitespace character is [ \t\n\x0B\f\r], that is a space, tab, newline,
	 * char(11), formfeed, or carriage return.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex whitespace() {
		return extend(new UnescapedSequence("\\s"));
	}

	/**
	 * Adds a check for a non-whitespace character(\w) to the regular expression
	 * without grouping.
	 *
	 * <p>
	 * A whitespace character is [ \t\n\x0B\f\r], that is a space, tab, newline,
	 * char(11), form-feed, or carriage return. A non-whitespace character is
	 * anything else.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex nonWhitespace() {
		return extend(new UnescapedSequence("\\S"));
	}

	@Override
	public Regex wordBoundary() {
		return extend(new UnescapedSequence("\\b"));
	}

	@Override
	public Regex nonWordBoundary() {
		return extend(new UnescapedSequence("\\B"));
	}

	@Override
	public Regex theBeginningOfTheInput() {
		return extend(new UnescapedSequence("\\A"));
	}

	@Override
	public Regex theEndOfThePreviousMatch() {
		return extend(new UnescapedSequence("\\G"));
	}

	@Override
	public Regex theEndOfTheInput() {
		return extend(new UnescapedSequence("\\z"));
	}

	@Override
	public Regex theEndOfTheInputButForTheFinalTerminator() {
		return extend(new UnescapedSequence("\\Z"));
	}

	/**
	 * Adds a check for a space character( ) to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex space() {
		return extend(new UnescapedSequence(" "));
	}

	/**
	 * Places the regular expression in a capturing group.
	 *
	 * <p>
	 * capturing and grouping are the same, there are methods of both names to
	 * capture the intent.
	 *
	 * @param regexp
	 * @return a new regexp
	 */
	@Override
	public Regex capture(Regex regexp) {
		return new UnescapedSequence("(" + regexp.getRegexp() + ")");
	}

	/**
	 * Adds a check for a negative integer to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * Will capture the minus so watch out for that in your calculator
	 * application.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex negativeInteger() {
		return extend(literal('-').anyBetween('1', '9').once().digit().zeroOrMore());
	}

	/**
	 * Adds a check for a positive integer to the regular expression without
	 * grouping.
	 *
	 * <p>
	 * Will capture the plus so watch out for that in your calculator application.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex positiveInteger() {
		return extend(
				startingAnywhere()
						.notPrecededBy("-")
						.plus().onceOrNotAtAll()
						.anyBetween('1', '9').once()
						.digit().zeroOrMore()
		);
	}

	/**
	 * Adds a check for a positive or negative integer to the regular expression
	 * without grouping.
	 *
	 * <p>
	 * Will capture the plus or minus so watch out for that in your calculator
	 * application.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex integer() {
		return extend(Regex
				.startGroup().literal("-")
				.or().literal("+")
				.closeGroup().onceOrNotAtAll()
				.anyBetween('1', '9').once().digit().zeroOrMore()
		);
	}

	/**
	 * Adds a standard pattern that will match any valid number to the pattern as
	 * a grouped element.
	 *
	 * <p>
	 * A valid number is any sequence of digits not starting with zero, optionally
	 * preceded with a plus or minus, and optionally followed by a decimal point
	 * and a sequence of digits, that is clearly separated from other characters.
	 *
	 * <p>
	 * An example of a valid number would be +2.345.
	 *
	 * <p>
	 * Invalid numbers include 02.345, A4, _234, 2*E10, and 5678ABC.
	 *
	 * @return the current regex with a number matching pattern added to it
	 */
	@Override
	public Regex number() {
		return extend(
				Regex.startGroup()
						.anyOf("-+").onceOrNotAtAll()
						.wordBoundary()
						.anyBetween('1', '9').atLeastOnce()
						.digit().zeroOrMore()
						.add(startingAnywhere()
								.dot().once()
								.digit().oneOrMore()
						).onceOrNotAtAll()
						.notFollowedBy(Regex.startingAnywhere().nonWhitespace())
						.closeGroup()
		);
	}

	/**
	 * Adds a standard pattern that will match any number-like sequence to the
	 * pattern as a grouped element.
	 *
	 * <p>
	 * A number-like sequence is any sequence of digits, optionally preceded with
	 * a plus or minus, and optionally followed by a decimal point and a sequence
	 * of digits.
	 *
	 * <p>
	 * It differs from a number in that zero can be the first digit and the
	 * sequence doesn't need to be clearly separated from the surrounding
	 * characters.
	 *
	 * <p>
	 * It differs from digits in that leading +/- and a middle decimal point are
	 * included.
	 *
	 * <p>
	 * A valid match would occur for the following +2.345, 02.345, A4, _234,
	 * _234.5, 2*E10, and 5678ABC.
	 *
	 * @return the current regex with a number matching pattern added to it
	 */
	@Override
	public Regex numberLike() {
		return extend(
				Regex.startGroup()
						.anyOf("-+").onceOrNotAtAll()
						.digit().atLeastOnce()
						.add(startingAnywhere()
								.dot().once()
								.digit().oneOrMore()
						).onceOrNotAtAll()
						.closeGroup()
		);
	}

	/**
	 * Adds a check for a simple range to the regular expression without grouping.
	 *
	 * <p>
	 * To add more complex ranges use .add(new Regex.Range(lowest, highest)).
	 *
	 * @param lowest the (inclusive) start of the character range
	 * @param highest the (inclusive) end of the character range
	 * @return a new regexp
	 */
	@Override
	public Regex anyBetween(Character lowest, Character highest) {
		return extend(startingAnywhere().openRange(lowest, highest).closeRange());
	}

	/**
	 * Adds a check for a simple range to the regular expression without grouping.
	 *
	 * <p>
	 * To add more complex ranges use .add(new Regex.Range(rangeItems)).
	 *
	 * @param literals all the characters to be included in the range, for example
	 * "abcdeABCDE"
	 * @return a new regexp
	 */
	@Override
	public Regex anyOf(String literals) {
		return extend(startingAnywhere().openRange(literals).closeRange());
	}

	/**
	 * Adds a check to exclude a simple range from the regular expression without
	 * grouping.
	 *
	 * <p>
	 * To add more complex ranges use .add(new Regex.Range(lowest, highest)).
	 *
	 * @param lowest the (inclusive) start of the character range
	 * @param highest the (inclusive) end of the character range
	 * @return a new regexp
	 */
	@Override
	public Regex nothingBetween(Character lowest, Character highest) {
		return extend(startingAnywhere().openRange(lowest, highest).negated().closeRange());
	}

	/**
	 * Adds a check to exclude a simple range from the regular expression without
	 * grouping.
	 *
	 * <p>
	 * To add more complex ranges use .add(new Regex.Range(rangeItems)).
	 *
	 * @param literals all the characters to be included in the range, for example
	 * "abcdeABCDE"
	 * @return a new regexp
	 */
	@Override
	public Regex noneOf(String literals) {
		return extend(startingAnywhere().openRange(literals).negated().closeRange());
	}

	/**
	 * Places the regular expression in a group and adds it as one element for the
	 * next instruction.
	 *
	 * <p>
	 * capturing and grouping are the same, there are methods of both names to
	 * capture the intent.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex groupEverythingBeforeThis() {
		return new Group(this);
	}

	/**
	 * Places the regular expression in a group and add it as one element for the
	 * next instruction.
	 *
	 * <p>
	 * capturing and grouping are the same, there are methods of both names to
	 * capture the intent.
	 *
	 * @return a new regexp
	 */
	@Override
	public Regex addGroup(Regex regex) {
		return this.extend(regex.groupEverythingBeforeThis());
	}

	protected final Pattern getCompiledVersion() {
		if (compiledVersion == null) {
			final String regexp = this.getRegexp();
			compiledVersion = Pattern.compile(regexp);
		}
		return compiledVersion;
	}

	public boolean matchesEntireString(String string) {
		return getCompiledVersion().matcher(string).matches();
	}

	public boolean matchesWithinString(String string) {
		return getCompiledVersion().matcher(string).find();
	}

	public Stream<MatchResult> getResultsStream(String string) {
		return getCompiledVersion().matcher(string).results();
	}

	public String getFirstMatch(String string) {
		return getCompiledVersion().matcher(string).group();
	}

	public MatchResult getMatchResult(String string) {
		return getCompiledVersion().matcher(string).toMatchResult();
	}

	/**
	 * Adds a check for a that the next element does not have the literal value
	 * before it.
	 *
	 * <p>
	 * For instance a positive integer is an integer that may have a plus in front
	 * of it but definitely isn't preceded by a minus. So it uses a notPrecededBy:
	 * startingAnywhere().notPrecededBy("-").plus().onceOrNotAtAll()...
	 *
	 * @param literalValue
	 * @return a new regexp
	 */
	@Override
	public Regex notPrecededBy(String literalValue) {
		return this.notPrecededBy(new LiteralSequence(literalValue));
	}

	/**
	 * Adds a check for a that the next element does not have the literal value
	 * before it.
	 *
	 * <p>
	 * For instance a positive integer is an integer that may have a plus in front
	 * of it but definitely isn't preceded by a minus. So it uses a notPrecededBy:
	 * startingAnywhere().notPrecededBy("-").plus().onceOrNotAtAll()...
	 *
	 * @param literalValue
	 * @return a new regexp
	 */
	@Override
	public Regex notPrecededBy(Regex literalValue) {
		return this
				.extend(new UnescapedSequence("(?<!"))
				.extend(literalValue)
				.extend(new UnescapedSequence(")"));
	}

	/**
	 * Adds a check for a that the next element does not have the literal value
	 * immediately after it.
	 *
	 * <p>
	 * For instance to match words but not e-mail addresses you might use
	 * Regex.startingAnywhere().word().notFollowedBy("@").
	 *
	 * @param literalValue
	 * @return a new regexp
	 */
	@Override
	public Regex notFollowedBy(String literalValue) {
		return this.notFollowedBy(new LiteralSequence(literalValue));
	}

	/**
	 * Adds a check for a that the next element does not have the literal value
	 * immediately after it.
	 *
	 * <p>
	 * For instance to match words but not e-mail addresses you might use
	 * Regex.startingAnywhere().word().notFollowedBy("@").
	 *
	 * @param literalValue
	 * @return a new regexp
	 */
	@Override
	public Regex notFollowedBy(Regex literalValue) {
		return this
				.extend(new UnescapedSequence("(?!"))
				.extend(literalValue)
				.extend(new UnescapedSequence(")"));
	}

	//(?!@)
	/**
	 * Starts making a character range, use {@link RangeBuilder#closeRange() } to
	 * return to the regex.
	 *
	 * <p>
	 * This provides more options than the {@link #anyBetween(java.lang.Character, java.lang.Character)
	 * } and {@link #anyOf(java.lang.String) } methods for creating ranges.
	 *
	 * @param lowest
	 * @param highest
	 * @return the start of a range.
	 */
	public RangeBuilder openRange(char lowest, char highest) {
		return new RangeBuilder(this, lowest, highest);
	}

	public RangeBuilder openRange(String literals) {
		return new RangeBuilder(this, literals);
	}

	/**
	 * Create a regular expression that includes all the regexps supplied within
	 * an OR grouping.
	 *
	 * <p>
	 * for instance, use this to generate "(FRED|EMILY|GRETA|DONALD)".
	 *
	 * <p>
	 * {@code Regex regex =  Regex.startGroup().literal("A").or().literal("B").closeGroup();
	 * } produces "(A|B)".
	 *
	 * @return a new regular expression
	 */
	public static RegexGroup.Or startGroup() {
		return new RegexGroup.Or(startingAnywhere());
	}

	/**
	 * Extends this regular expression with an OR grouping.
	 *
	 * <p>
	 * for instance, use this to generate "(FRED|EMILY|GRETA|DONALD)".
	 *
	 * <p>
	 * {@code Regex regex =  Regex.startAnywhere().literal("Project ").startGroup().literal("A").or().literal("B").closeGroup();
	 * } produces "Project (A|B)".
	 *
	 * @return a new regular expression
	 */
	public RegexGroup.Or openGroup() {
		return new RegexGroup.Or(this);
	}

	public List<String> getAllMatches(String string) {
		Matcher matcher = this.getCompiledVersion().matcher(string);
		List<String> matches = matcher.results().map(m -> m.group()).collect(Collectors.toList());
		return matches;
	}

	@Override
	public Regex literalCaseInsensitive(String literal) {
		return this
				.addGroup(Regex.startingAnywhere()
						.caseInsensitiveGroup()
						.literal(literal)
						.caseInsensitiveEnd()
				);
	}

	public RegexGroup.CaseInsensitive caseInsensitiveGroup() {
		return new RegexGroup.CaseInsensitive(this);
	}

	public static class SingleCharacter extends Regex {

		private final Character literal;

		protected SingleCharacter(Character character) {
			this.literal = character;
		}

		@Override
		public String getRegexp() {
			return "" + literal;
		}
	}

	public static class LiteralSequence extends Regex {

		private final String literal;

		public LiteralSequence(String literals) {
			if (literals == null) {
				this.literal = "";
			} else {
				this.literal = literals
						.replaceAll("\\\\", "\\")
						.replaceAll("\\.", "\\.")
						.replaceAll("\\?", "\\?")
						.replaceAll("\\+", "\\\\+")
						.replaceAll("\\*", "\\*")
						.replaceAll("\\^", "\\^")
						.replaceAll("\\$", "\\$")
						.replaceAll("\\|", "\\|")
						.replaceAll("\\[", "\\[");
			}
		}

		@Override
		public String getRegexp() {
			return "" + literal;
		}
	}

	protected static class UnescapedSequence extends Regex {

		private final String literal;

		protected UnescapedSequence(String literals) {
			if (literals == null) {
				this.literal = "";
			} else {
				this.literal = literals;
			}
		}

		@Override
		public String getRegexp() {
			return "" + literal;
		}
	}

	protected static class RegexpCombination extends Regex {

		private final Regex first;
		private final Regex second;

		protected RegexpCombination(Regex first, Regex second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public String getRegexp() {
			return first.getRegexp() + second.getRegexp();
		}
	}

//	private static class Or extends Regex {
//
//		private final SeparatedString sepString;
//
//		public Or(Regex first, Regex... regexps) {
//			sepString = SeparatedString
//					.forSeparator("|")
//					.withThisBeforeEachTerm("(")
//					.withThisAfterEachTerm(")")
//					.add(first.getRegexp())
//					.addAll(
//							(t) -> {
//								return t.getRegexp();
//							}, regexps);
//		}
//
//		@Override
//		public String getRegexp() {
//			return sepString.toString();
//		}
//	}
	public static class Group extends Regex {

		private final Regex regexp;

		public Group(Regex regexp) {
			this.regexp = regexp;
		}

		@Override
		public String getRegexp() {
			return "(" + regexp.getRegexp() + ")";
		}
	}

	public static class RangeBuilder {

		private final Regex origin;
		private String literals;

		private boolean negated = false;
		private boolean includeMinus = false;
		private boolean includeOpenBracket = false;
		private boolean includeCloseBracket = false;

		public RangeBuilder(Regex original) {
			this.origin = original;
		}

		public RangeBuilder(Regex original, Character lowest, Character highest) {
			this(original);
			addRange(lowest, highest);
		}

		public RangeBuilder(Regex original, String literals) {
			this(original);
			addLiterals(literals);
		}

		protected final RangeBuilder addRange(Character lowest, Character highest) {
			this.literals = lowest + "-" + highest;
			return this;
		}

		protected final RangeBuilder addLiterals(String literals1) {
			this.literals = literals1.replaceAll("-", "").replaceAll("]", "");
			this.includeMinus = literals1.contains("-");
			this.includeOpenBracket = literals1.contains("[");
			this.includeCloseBracket = literals1.contains("]");
			return this;
		}

		public RangeBuilder not() {
			this.negated = true;
			return this;
		}

		public RangeBuilder negated() {
			return not();
		}

		public RangeBuilder includeMinus() {
			includeMinus = true;
			return this;
		}

		public RangeBuilder and(Character lowest, Character highest) {
			return addRange(lowest, highest);
		}

		public RangeBuilder and(String literals) {
			return addLiterals(literals);
		}

		public RangeBuilder excluding(Character lowest, Character highest) {
			excluding(new RangeBuilder(Regex.startingAnywhere(), lowest, highest));
			return this;
		}

		public RangeBuilder excluding(String literals) {
			excluding(new RangeBuilder(Regex.startingAnywhere(), literals));
			return this;
		}

		public RangeBuilder excluding(RangeBuilder newRange) {
			this.literals = this.literals + "-" + newRange.encloseInBrackets();
			return this;
		}

		public String encloseInBrackets() {
			return "["
					+ (negated ? "^" : "")
					+ (includeMinus ? "-" : "")
					+ (includeOpenBracket ? "\\[" : "")
					+ (includeCloseBracket ? "\\]" : "")
					+ literals
					+ "]";
		}

		public Regex closeRange() {
			return origin.extend(new UnescapedSequence(encloseInBrackets()));
		}
	}

}
