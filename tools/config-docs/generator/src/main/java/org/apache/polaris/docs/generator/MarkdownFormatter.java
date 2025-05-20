/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.docs.generator;

import static java.util.Objects.requireNonNull;

import com.sun.source.doctree.AttributeTree;
import com.sun.source.doctree.DeprecatedTree;
import com.sun.source.doctree.DocCommentTree;
import com.sun.source.doctree.DocTree;
import com.sun.source.doctree.EndElementTree;
import com.sun.source.doctree.EntityTree;
import com.sun.source.doctree.ErroneousTree;
import com.sun.source.doctree.HiddenTree;
import com.sun.source.doctree.IndexTree;
import com.sun.source.doctree.LinkTree;
import com.sun.source.doctree.LiteralTree;
import com.sun.source.doctree.ReferenceTree;
import com.sun.source.doctree.SeeTree;
import com.sun.source.doctree.SinceTree;
import com.sun.source.doctree.StartElementTree;
import com.sun.source.doctree.SummaryTree;
import com.sun.source.doctree.TextTree;
import com.sun.source.doctree.UnknownBlockTagTree;
import com.sun.source.doctree.UnknownInlineTagTree;
import com.sun.source.doctree.ValueTree;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.VariableElement;
import org.apache.commons.text.StringEscapeUtils;

public abstract class MarkdownFormatter {
  private final Element element;
  private String javadocDeprecated;
  private String javadocSee;
  private String javadocHidden;
  private String javadocSince;
  private String javadocSummary;
  private String javadocBody;

  public MarkdownFormatter(Element element, DocCommentTree commentTree) {
    this.element = element;

    if (commentTree != null) {

      // @see, @deprecated, etc
      for (DocTree doc : commentTree.getBlockTags()) {
        switch (doc.getKind()) {
          case SEE:
            {
              // @see
              var seeTree = (SeeTree) doc;
              var reference = seeTree.getReference();
              this.javadocSee = format(reference);
            }
            break;
          case DEPRECATED:
            {
              // @deprecated
              var deprecatedTree = (DeprecatedTree) doc;
              var body = deprecatedTree.getBody();
              this.javadocDeprecated = format(body);
            }
            break;
          case HIDDEN:
            {
              // @hidden
              var hiddenTree = (HiddenTree) doc;
              var body = hiddenTree.getBody();
              this.javadocHidden = format(body);
            }
            break;
          case SINCE:
            {
              // @since
              var sinceTree = (SinceTree) doc;
              var body = sinceTree.getBody();
              this.javadocSince = format(body);
            }
            break;
          default:
            break;
        }
      }

      var fullBody = commentTree.getFullBody();
      if (!fullBody.isEmpty()) {
        var body =
            switch (fullBody.getFirst().getKind()) {
              case SUMMARY -> {
                var summaryTree = (SummaryTree) fullBody.getFirst();
                this.javadocSummary = format(summaryTree.getSummary());
                yield fullBody.subList(1, fullBody.size());
              }
              default -> {
                this.javadocSummary = format(commentTree.getFirstSentence());
                yield commentTree.getBody();
              }
            };
        this.javadocBody = format(body);
      }
    }
  }

  public String description() {
    var sb = new StringBuilder();
    if (javadocSummary != null) {
      sb.append(javadocSummary);
    }
    if (javadocBody != null) {
      sb.append(' ').append(javadocBody);
    }
    if (javadocSince != null) {
      sb.append("\n\nSince: ").append(javadocSince);
    }
    if (javadocSee != null) {
      sb.append("\n\nSee: ").append(javadocSee);
    }
    if (javadocDeprecated != null) {
      sb.append("\n\n_Deprecated_ ").append(javadocDeprecated);
    } else if (element != null) {
      var deprecated = element.getAnnotation(Deprecated.class) != null;
      if (deprecated) {
        sb.append("\n\n_Deprecated_ ");
      }
    }
    return sb.toString();
  }

  public boolean isHidden() {
    return javadocHidden != null;
  }

  private String format(List<? extends DocTree> docTrees) {
    var s = new MDFormat().formatList(docTrees);
    do {
      var r = s.replaceAll("\n\n\n", "\n\n");
      if (r.equals(s)) {
        return s;
      }
      s = r;
    } while (true);
  }

  static class RootTarget extends Target {
    RootTarget() {
      super("");
    }
  }

  abstract static class ListTarget extends Target {
    final String itemPrefix;

    public ListTarget(String itemPrefix, String indent) {
      super(indent);
      this.itemPrefix = itemPrefix;
    }

    ListItemTarget newItem() {
      return new ListItemTarget(indent);
    }
  }

  static class OrderedListTarget extends ListTarget {
    OrderedListTarget(String indent) {
      super("\n" + indent + " 1. ", indent + "    ");
    }
  }

  static class UnorderedListTarget extends ListTarget {
    UnorderedListTarget(String indent) {
      super("\n" + indent + " * ", indent + "   ");
    }
  }

  static class ListItemTarget extends Target {
    ListItemTarget(String indent) {
      super(indent);
    }
  }

  static class ATagTarget extends Target {
    final Map<String, String> attributes;

    ATagTarget(String indent, Map<String, String> attributes) {
      super(indent);
      this.attributes = attributes;
    }
  }

  abstract static class Target {
    final StringBuilder text = new StringBuilder();
    final String indent;

    Target(String indent) {
      this.indent = indent;
    }

    void addText(String text) {
      addTextOrCode(text, false);
    }

    void addTextOrCode(String text, boolean code) {
      var t = text.replaceAll("\n", " ");
      t = t.replaceFirst("^\\s*", "");

      if (t.isEmpty() && !text.isEmpty()) {
        maybeAddSeparator();
        return;
      }

      var e = text.replaceFirst("\\s*$", "");

      if (text.charAt(0) != t.charAt(0)) {
        maybeAddSeparator();
      }

      if (code) {
        this.text.append('`');
      }
      this.text.append(t);
      if (code) {
        this.text.append('`');
      }

      if (!e.equals(t)) {
        maybeAddSeparator();
      }
    }

    void addCode(String text) {
      addTextOrCode(text, true);
    }

    void maybeAddSeparator() {
      var len = text.length();
      if (len == 0) {
        return;
      }
      if (Character.isWhitespace(text.charAt(len - 1))) {
        return;
      }
      text.append(' ');
    }

    void trimRight() {
      var l = text.length();
      while (l > 0 && Character.isWhitespace(text.charAt(l - 1))) {
        text.setLength(--l);
      }
    }
  }

  private class MDFormat {
    final Deque<Target> stack = new ArrayDeque<>();

    String formatList(List<? extends DocTree> docTrees) {
      var root = new RootTarget();
      stack.add(root);
      process(docTrees);
      return root.text.toString();
    }

    private void process(List<? extends DocTree> docTrees) {
      for (var docTree : docTrees) {
        process(docTree);
      }
    }

    private void process(DocTree doc) {
      var target = requireNonNull(stack.peekLast());
      switch (doc.getKind()) {
        case DOC_COMMENT:
          {
            var docCommentTree = (DocCommentTree) doc;

            var first = docCommentTree.getFirstSentence();
            for (var ch : first) {
              process(ch);
            }
            var body = docCommentTree.getBody();
            for (var ch : body) {
              process(ch);
            }

            // `block` has all the `@see` and such
            var block = docCommentTree.getBlockTags();
            process(block);
          }
          break;
        case COMMENT:
          {
            // CommentTree commentTree = (CommentTree) doc;
            // String body = commentTree.getBody();
            // target.text.append(body);
          }
          break;

        case CODE:
          {
            // @code
            var literalTree = (LiteralTree) doc;
            var body = literalTree.getBody();
            target.addCode(body.getBody());
          }
          break;
        case LINK:
          {
            // @link
            link((LinkTree) doc, target, true);
          }
          break;
        case LINK_PLAIN:
          {
            // @linkplain
            link((LinkTree) doc, target, false);
          }
          break;
        case VALUE:
          {
            // @value
            value((ValueTree) doc, target);
          }
          break;
        case INDEX:
          {
            // @index
            var indexTree = (IndexTree) doc;
            // var description = indexTree.getDescription();
            var searchTerm = indexTree.getSearchTerm();
            process(searchTerm);
          }
          break;
        case SUMMARY:
          {
            // @summary (alternative to first sentence)
            var summaryTree = (SummaryTree) doc;
            var summary = summaryTree.getSummary();
            // no special handling here
            process(summary);
          }
          break;
        case DOC_ROOT:
        case DOC_TYPE:
        case INHERIT_DOC:
          // ignored "inline" tags
          break;

        case ENTITY:
          {
            // HTML entity
            var entityTree = (EntityTree) doc;
            var unescaped = StringEscapeUtils.unescapeHtml4(entityTree.toString());
            target.addText(unescaped);
          }
          break;
        case IDENTIFIER:
          {
            // identifier
            // IdentifierTree identifierTree = (IdentifierTree) doc;
            // Name name = identifierTree.getName();
          }
          break;
        case REFERENCE:
          {
            // reference tree
            var referenceTree = (ReferenceTree) doc;
            var signature = referenceTree.getSignature();
            target.text.append(signature);
          }
          break;

        case TEXT:
          {
            var textTree = (TextTree) doc;
            // TODO process HTML entities ?
            target.addText(textTree.getBody());
          }
          break;
        case LITERAL:
          {
            var literalTree = (LiteralTree) doc;
            var textTree = literalTree.getBody();
            target.addText(textTree.getBody());
          }
          break;
        case ERRONEOUS:
          {
            // invalid text
            var erroneousTree = (ErroneousTree) doc;
            target.addText(erroneousTree.getBody());
          }
          break;

        case UNKNOWN_BLOCK_TAG:
          {
            var unknownBlockTagTree = (UnknownBlockTagTree) doc;
            var content = unknownBlockTagTree.getContent();
            process(content);
          }
          break;
        case UNKNOWN_INLINE_TAG:
          {
            var unknownInlineTagTree = (UnknownInlineTagTree) doc;
            var content = unknownInlineTagTree.getContent();
            process(content);
          }
          break;
        case START_ELEMENT:
          {
            // start HTML element
            var startElementTree = (StartElementTree) doc;
            var name = startElementTree.getName();
            var attributes = startElementTree.getAttributes();
            // boolean selfClosing = startElementTree.isSelfClosing();

            var attributeMap =
                attributes.stream()
                    .filter(d -> d.getKind() == DocTree.Kind.ATTRIBUTE)
                    .map(AttributeTree.class::cast)
                    .collect(
                        Collectors.toMap(
                            a -> a.getName().toString().toLowerCase(Locale.ROOT),
                            a -> format(a.getValue())));

            switch (name.toString().toLowerCase(Locale.ROOT)) {
              case "p":
                target.text.append("\n\n").append(target.indent);
                break;
              case "a":
                target.text.append('[');
                stack.addLast(new ATagTarget(target.indent, attributeMap));
                break;
              case "em":
              case "i":
                target.text.append('_');
                break;
              case "b":
                target.text.append("**");
                break;
              case "ol":
                target.text.append("\n\n");
                stack.addLast(new OrderedListTarget(target.indent));
                break;
              case "ul":
                target.text.append("\n\n");
                stack.addLast(new UnorderedListTarget(target.indent));
                break;
              case "li":
                while (!(target instanceof ListTarget listTarget)) {
                  var last = stack.removeLast();
                  target = stack.peekLast();
                  requireNonNull(target).text.append(last.text);
                }
                target.text.append(listTarget.itemPrefix);
                stack.addLast(listTarget.newItem());
                break;
              case "code":
                target.text.append('`');
                break;
              default:
                break;
            }

            process(attributes);
          }
          break;
        case END_ELEMENT:
          {
            // end HTML element
            var endElementTree = (EndElementTree) doc;
            var name = endElementTree.getName();
            switch (name.toString().toLowerCase(Locale.ROOT)) {
              case "p":
                // noop
                break;
              case "a":
                var a = (ATagTarget) stack.removeLast();
                target = stack.peekLast();
                requireNonNull(target).addText(a.text.toString());
                target.text.append("](").append(a.attributes.get("href")).append(")");
                break;
              case "em":
              case "i":
                target.trimRight();
                target.text.append('_');
                break;
              case "b":
                target.trimRight();
                target.text.append("**");
                break;
              case "ol":
              case "ul":
                while (!(target instanceof ListTarget)) {
                  var last = stack.removeLast();
                  target = stack.peekLast();
                  requireNonNull(target).text.append(last.text);
                }
                var list = stack.removeLast();
                target = stack.peekLast();
                requireNonNull(target).text.append(list.text).append("\n\n");
                break;
              case "li":
                while (!(target instanceof ListTarget)) {
                  var last = stack.removeLast();
                  target = stack.peekLast();
                  requireNonNull(target).text.append(last.text);
                }
                requireNonNull(stack.peekLast()).text.append(stack.removeLast().text);
                break;
              case "code":
                target.text.append('`');
                break;
              default:
                break;
            }
          }
          break;
        case ATTRIBUTE:
          // Attribute of an HTML tag
          break;

        // "block" tags
        case AUTHOR:
        case DEPRECATED:
        case EXCEPTION:
        case HIDDEN:
        case PARAM:
        case PROVIDES:
        case RETURN:
        case SEE:
        case SERIAL:
        case SERIAL_DATA:
        case SERIAL_FIELD:
        case SINCE:
        case THROWS:
        case USES:
        case VERSION:
          // ignored "block" tags
          break;

        case OTHER:
        default:
          break;
      }
    }

    private void value(ValueTree valueTree, Target target) {
      // TODO resolve reference properly
      var reference = valueTree.getReference();
      var signature = reference.getSignature().trim();
      if (signature.startsWith("#") && element != null) {
        var referenced =
            element.getEnclosingElement().getEnclosedElements().stream()
                .filter(enc -> enc.getSimpleName().toString().equals(signature.substring(1)))
                .findFirst();
        if (referenced.isPresent()) {
          var ref = referenced.get();
          if (ref instanceof VariableElement variableElement) {
            var value = variableElement.getConstantValue();
            if (value instanceof String) {
              target.text.append('"').append(value).append('"');
            } else {
              target.text.append(value);
            }
          }
        } else {
          target.text.append(signature);
        }
      } else {
        process(reference);
      }
    }

    private void link(LinkTree linkTree, Target target, boolean codeValue) {
      // TODO resolve reference properly

      var label = linkTree.getLabel();
      if (!label.isEmpty()) {
        process(label);
      }

      var reference = linkTree.getReference();
      var signature = reference.getSignature().trim();
      target.maybeAddSeparator();
      target.text.append("(");
      if (codeValue) {
        target.text.append('`');
      }
      if (signature.startsWith("#") && element != null) {
        var referenced =
            element.getEnclosingElement().getEnclosedElements().stream()
                .filter(enc -> enc.getSimpleName().toString().equals(signature.substring(1)))
                .findFirst();
        if (referenced.isPresent()) {
          var ref = referenced.get();
          if (ref instanceof VariableElement variableElement) {
            var value = variableElement.getConstantValue();
            target.text.append(value);
          }
        } else {
          target.text.append(signature);
        }
      } else {
        process(reference);
      }
      if (codeValue) {
        target.text.append('`');
      }
      target.text.append(')');
    }
  }
}
