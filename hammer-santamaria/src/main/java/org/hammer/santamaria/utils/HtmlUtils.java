package org.hammer.santamaria.utils;

/**
 * Html SantaMaria Utils
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer Project -Pinta
 *
 */
public class HtmlUtils {
	private static String pattern;

	private final static String[] tagsTab = { "!doctype", "a", "abbr", "acronym", "address", "applet", "area",
			"article", "aside", "audio", "b", "base", "basefont", "bdi", "bdo", "bgsound", "big", "blink", "blockquote",
			"body", "br", "button", "canvas", "caption", "center", "cite", "code", "col", "colgroup", "content", "data",
			"datalist", "dd", "decorator", "del", "details", "dfn", "dir", "div", "dl", "dt", "element", "em", "embed",
			"fieldset", "figcaption", "figure", "font", "footer", "form", "frame", "frameset", "h1", "h2", "h3", "h4",
			"h5", "h6", "head", "header", "hgroup", "hr", "html", "i", "iframe", "img", "input", "ins", "isindex",
			"kbd", "keygen", "label", "legend", "li", "link", "listing", "main", "map", "mark", "marquee", "menu",
			"menuitem", "meta", "meter", "nav", "nobr", "noframes", "noscript", "object", "ol", "optgroup", "option",
			"output", "p", "param", "plaintext", "pre", "progress", "q", "rp", "rt", "ruby", "s", "samp", "script",
			"section", "select", "shadow", "small", "source", "spacer", "span", "strike", "strong", "style", "sub",
			"summary", "sup", "table", "tbody", "td", "template", "textarea", "tfoot", "th", "thead", "time", "title",
			"tr", "track", "tt", "u", "ul", "var", "video", "wbr", "xmp" };

	static {
		StringBuffer tags = new StringBuffer();
		for (int i = 0; i < tagsTab.length; i++) {
			tags.append(tagsTab[i].toLowerCase()).append('|').append(tagsTab[i].toUpperCase());
			if (i < tagsTab.length - 1) {
				tags.append('|');
			}
		}
		pattern = "</?(" + tags.toString() + "){1}.*?/?>";
	}

	public static String sanitize(String input) {
		return input.replaceAll(pattern, "");
	}

	public final static void main(String[] args) {
		System.out.println(HtmlUtils.pattern);

		System.out.println(HtmlUtils.sanitize("<font><p>some text</p><br/> <p>another text</p></font>"));
	}
}
