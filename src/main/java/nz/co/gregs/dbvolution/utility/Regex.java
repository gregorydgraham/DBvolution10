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

import java.util.regex.Pattern;

/**
 *
 * @author gregorygraham
 */
public abstract class Regex {

	private Pattern compiledVersion;

	private Regex() {
	}

	public abstract String getRegexp();

	/**
	 * Creat a new empty regular expression.
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
	 * Create a regular expression that includes all the regexps supplied within
	 * an OR grouping.
	 *
	 * <p>
	 * for instance, use this to generate "(FRED|EMILY|GRETA|DONALD)".
	 *
	 * @param regexp
	 * @param regexps
	 * @return a new regular expression
	 */
	public static Regex or(Regex regexp, Regex... regexps) {
		return new Or(regexp, regexps);
	}

	/**
	 * Adds the regular expression to the end of current expression as a new
	 * group.
	 *
	 * <p>
 For example Regex.startingAnywhere().add(allowedValue).add(separator) will
 add the "separator" regular expression to the "allowedValue" expression
 (the rest of the instruction adds nothing). Assuming that allowedValue is
 "[0-9]" and separator is ":", the full regexp will be "([0-9])(:)".
	 *
	 * @param second
	 * @return a new regular expression consisting of the current expression and
	 * the supplied expression added together
	 */
	public Regex add(Regex second) {
		return new RegexpCombination(this, second.group());
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
 For example
 Regex.startingAnywhere().extend(allowedValue).extend(separator) will add
 the "separator" regular expression to the "allowedValue" expression (the
 rest of the instruction adds nothing). Assuming that allowedValue is
 "[0-9]" and separator is ":", the full regexp will be "[0-9]:".
	 *
	 * @param second
	 * @return a new regular expression consisting of the current expression and
	 * the supplied expression added together
	 */
	public Regex extend(Regex second) {
		return new RegexpCombination(this, second);
	}

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
	public Regex literal(String literals) {
		return extend(new LiteralSequence(literals));
	}

	/**
	 * Adds a literal backslash(\) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex backslash() {
		return extend(new UnescapedSequence("\\\\"));
	}

	/**
	 * Adds a literal carat (^) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex carat() {
		return extend(new UnescapedSequence("\\^"));
	}

	/**
	 * Adds a literal dollar sign($) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex dollarSign() {
		return extend(new UnescapedSequence("\\$"));
	}

	/**
	 * Adds a literal dot(.) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex dot() {
		return extend(new UnescapedSequence("\\."));
	}

	/**
	 * Adds a literal question mark(?) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex questionMark() {
		return extend(new UnescapedSequence("\\?"));
	}

	/**
	 * Adds a literal plus sign(+) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex plus() {
		return extend(new UnescapedSequence("\\+"));
	}

	/**
	 * Adds a literal star(*) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex star() {
		return extend(new UnescapedSequence("\\*"));
	}

	/**
	 * Adds a literal asterisk(*) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex asterisk() {
		return extend(new UnescapedSequence("\\*"));
	}

	/**
	 * Adds a literal pipe(|) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex pipe() {
		return extend(new UnescapedSequence("\\|"));
	}

	/**
	 * Adds a literal square bracket([) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex squareBracket() {
		return extend(new UnescapedSequence("\\["));
	}

	/**
	 * Adds a literal bracket, i.e. "(", to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex bracket() {
		return extend(new UnescapedSequence("\\("));
	}

	/**
	 * Adds a tab character(\t) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex tab() {
		return extend(new UnescapedSequence("\\t"));
	}

	/**
	 * Adds a newline character(\n) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex newline() {
		return extend(new UnescapedSequence("\\n"));
	}

	/**
	 * Adds a carriage return character(\r) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex carriageReturn() {
		return extend(new UnescapedSequence("\\r"));
	}

	/**
	 * Adds a formfeed character(\f) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex formfeed() {
		return extend(new UnescapedSequence("\\f"));
	}

	/**
	 * Adds a bell character(\a) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex bell() {
		return extend(new UnescapedSequence("\\a"));
	}

	/**
	 * Adds a escape character(\e) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex escapeCharacter() {
		return extend(new UnescapedSequence("\\e"));
	}

	/**
	 * Adds a control character(\cX) to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex controlCharacter(String x) {
		return extend(new UnescapedSequence("\\c" + x));
	}

	/**
	 * Adds a match for any single character to the regexp without grouping it.
	 *
	 * @return a new regexp
	 */
	public Regex anyCharacter() {
		return extend(new UnescapedSequence("."));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly once.
	 *
	 * @return a new regexp
	 */
	public Regex once() {
		return extend(new UnescapedSequence("{1}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position exactly X number of times.
	 *
	 * @return a new regexp
	 */
	public Regex thisManyTimes(int x) {
		return extend(new UnescapedSequence("{" + x + "}"));
	}

	/**
	 * Alters the previous element in the regexp so that it only matches if the
	 * element appears in that position X times or more.
	 *
	 * @return a new regexp
	 */
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
	public Regex optionalMany() {
		return zeroOrMore();
	}

	/**
	 * Adds a check for the end of the string to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
	public Regex endOfTheString() {
		return extend(new UnescapedSequence("$"));
	}

	/**
	 * Adds a check for a digit(0123456789) to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
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
	public Regex digits() {
		return digit().oneOrMore();
	}

	/**
	 * Adds a check for anything other than a digit(0123456789) to the regular
	 * expression without grouping.
	 *
	 * @return a new regexp
	 */
	public Regex nondigit() {
		return extend(new UnescapedSequence("\\D"));
	}

	/**
	 * Adds a check for one or more of anything other than a digit(0123456789) to
	 * the regular expression without grouping.
	 *
	 * @return a new regexp
	 */
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
	public Regex nonWhitespace() {
		return extend(new UnescapedSequence("\\S"));
	}

	/**
	 * Adds a check for a space character( ) to the regular expression without
	 * grouping.
	 *
	 * @return a new regexp
	 */
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
	 * @return a new regexp
	 */
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
	public Regex integer() {
		return extend(Regex.or(new UnescapedSequence("-"), new UnescapedSequence("+")).onceOrNotAtAll().anyBetween('1', '9').once().digit().zeroOrMore());
	}

	public Regex number() {
		return extend(Regex.or(
				new LiteralSequence("-"),
				new LiteralSequence("+")
		).onceOrNotAtAll()
				.anyBetween('1', '9').atLeastOnce()
				.digit().zeroOrMore()
				.extend(startingAnywhere()
						.dot().once()
						.digit().oneOrMore()
				).onceOrNotAtAll()
		);
	}

	/**
	 * Adds a check for a simple range to the regular expression without grouping.
	 *
	 * <p>
 To add more complex ranges use .add(new Regex.Range(lowest, highest)).
	 *
	 * @param lowest the (inclusive) start of the character range
	 * @param highest the (inclusive) end of the character range
	 * @return a new regexp
	 */
	public Regex anyBetween(Character lowest, Character highest) {
		return extend(new Range(lowest, highest));
	}

	/**
	 * Adds a check for a simple range to the regular expression without grouping.
	 *
	 * <p>
 To add more complex ranges use .add(new Regex.Range(rangeItems)).
	 *
	 * @param literals all the characters to be included in the range, for example
	 * "abcdeABCDE"
	 * @return a new regexp
	 */
	public Regex anyOf(String literals) {
		return extend(new Range(literals));
	}

	/**
	 * Places the regular expression in a group and making them one element for
	 * the next instruction.
	 *
	 * <p>
	 * capturing and grouping are the same, there are methods of both names to
	 * capture the intent.
	 *
	 * @return a new regexp
	 */
	public Regex.Group group() {
		return new Group(this);
	}

	protected final Pattern getCompiledVersion() {
		if (compiledVersion == null) {
			final String regexp = this.getRegexp();
			System.out.println("nz.co.gregs.dbvolution.utility.Regexp.matchesEntireString(): " + regexp);
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
	public Regex notPrecededBy(String literalValue) {
		return new UnescapedSequence("(?<!")
				.extend(new LiteralSequence(literalValue))
				.extend(new UnescapedSequence(")"));
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
						.replaceAll("\\+", "\\+")
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

	public static class Range extends Regex {

		private final String literals;

		public Range(Character lowest, Character highest) {
			this.literals = lowest + "-" + highest;
		}

		public Range(String literals) {
			this.literals = literals;
		}

		public Regex not() {
			return new NegatedCharacterRange(this);
		}

		public Regex negated() {
			return new NegatedCharacterRange(this);
		}

		public Range includeMinus() {
			return new Range("-" + this.literals);
		}

		public Range and(Character lowest, Character highest) {
			return and(new Range(lowest, highest));
		}

		public Range and(String literals) {
			return and(new Range(literals));
		}

		public Range and(Range range) {
			return new Range(this.literals + range.getRange());
		}

		public Range excluding(Range range) {
			return new Range(this.literals + "-[" + range.getRange() + "]");
		}

		@Override
		public String getRegexp() {
			return "[" + getRange() + "]";
		}

		public String getRange() {
			return literals;
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

	public static class NegatedCharacterRange extends Regex {

		private final Range range;

		public NegatedCharacterRange(Range range) {
			this.range = range;
		}

		@Override
		public String getRegexp() {
			return "[^" + range.getRange() + "]";
		}

	}

	private static class Or extends Regex {

		private final SeparatedString sepString;

		public Or(Regex first, Regex... regexps) {
			sepString = SeparatedString
					.forSeparator("|")
					.withWrapBefore("(")
					.withWrapAfter(")")
					.add(first.getRegexp())
					.addAll(
							(t) -> {
								return t.getRegexp();
							}, regexps);
		}

		@Override
		public String getRegexp() {
			return sepString.toString();
		}
	}

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

}
