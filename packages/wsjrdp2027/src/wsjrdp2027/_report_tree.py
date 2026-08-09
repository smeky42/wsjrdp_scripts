"""Generic, renderer-agnostic report tree.

A *report* is a tree of :class:`ReportNode` objects. A node *consists of parts*
(see :class:`ReportContent`) -- multi-line content pieces whose ``kind`` (plain
text / markdown / source code with a ``language``) lets a rich renderer show each
part appropriately -- plus child nodes.

:meth:`ReportTree.full_description` always renders **plain Unicode text**: no
colours, no fonts, using Unicode box-drawing characters for structure only.

For an interactive view, :func:`build_report_tree_widget` / :func:`show_report_tree`
map a :class:`ReportTree` onto a ``textual`` ``Tree`` widget. Because textual tree
nodes are single-line, a node's parts are rendered and split into lines: every
line but the last is added as a (prefixed) leaf, and the last line becomes the
node that owns the subtree. The interactive collapsed/expanded state lives on that
widget, not in this model.
"""

from __future__ import annotations

import dataclasses as _dataclasses
import typing as _typing


if _typing.TYPE_CHECKING:
    from textual.widget import Widget as _Widget


ReportContentKind = _typing.Literal["text", "markdown", "code", "warning"]


@_dataclasses.dataclass(frozen=True)
class ReportContent:
    """One *part* of a :class:`ReportNode`, kept renderer-agnostic.

    ``kind`` tells a rich renderer how to display ``text`` (as plain text, as
    Markdown, as syntax-highlighted source code in ``language`` -- e.g. ``"sql"``
    -- or as a ``"warning"`` shown in bold red). Plain-text rendering
    (:meth:`ReportTree.full_description`) ignores ``kind``/``language`` and emits
    ``text`` verbatim.
    """

    text: str
    kind: ReportContentKind = "text"
    language: str | None = None


class ReportNode:
    """A report node made of one or more *parts* plus optional child nodes.

    Each positional argument is a part: a plain ``str`` (wrapped into a
    ``kind="text"`` :class:`ReportContent`) or a :class:`ReportContent`. All parts
    are stored together in :attr:`parts`.
    """

    parts: tuple[ReportContent, ...]
    children: tuple[ReportNode, ...]

    __slots__ = ("children", "parts")

    def __init__(
        self,
        *parts: str | ReportContent,
        children: _typing.Iterable[ReportNode] = (),
    ) -> None:
        self.parts = tuple(
            part if isinstance(part, ReportContent) else ReportContent(part)
            for part in parts
        )
        self.children = tuple(children)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(parts={self.parts!r}, children={self.children!r})"


@_dataclasses.dataclass(frozen=True)
class ReportTree:
    """A report, i.e. a tree rooted at :attr:`root`."""

    root: ReportNode

    def full_description(self) -> str:
        """Render the whole tree as plain Unicode text (everything shown).

        Uses Unicode box-drawing characters for the tree structure only; the
        result carries no colours or fonts.
        """
        lines: list[str] = []
        _render_text(self.root, "", is_last=True, is_root=True, out=lines)
        return "\n".join(lines)


def _plain_lines(node: ReportNode) -> list[str]:
    if not node.parts:
        return [""]
    return "\n".join(part.text for part in node.parts).split("\n")


def _render_text(
    node: ReportNode,
    prefix: str,
    *,
    is_last: bool,
    is_root: bool,
    out: list[str],
) -> None:
    plines = _plain_lines(node)
    if is_root:
        out.extend(plines)
        child_prefix = ""
    else:
        connector = "└─ " if is_last else "├─ "
        cont = "   " if is_last else "│  "
        out.append(f"{prefix}{connector}{plines[0]}")
        out.extend(f"{prefix}{cont}{line}" for line in plines[1:])
        child_prefix = prefix + cont

    n = len(node.children)
    for i, child in enumerate(node.children):
        _render_text(child, child_prefix, is_last=(i == n - 1), is_root=False, out=out)


# --- textual rendering -------------------------------------------------------


def _rich_lines(node: ReportNode) -> list:
    """Render *node*'s parts to a list of single-line Rich ``Text`` objects.

    ``kind="code"`` parts are syntax-highlighted via Rich's ``Syntax`` (backed by
    Pygments); other kinds are plain text.
    """
    from rich.text import Text

    if not node.parts:
        return [Text("")]

    segments = []
    for part in node.parts:
        if part.kind == "code":
            from rich.syntax import Syntax

            seg = Syntax(
                part.text, part.language or "text", theme="ansi_dark", word_wrap=False
            ).highlight(part.text)
            seg.rstrip()
        elif part.kind == "warning":
            seg = Text(part.text, style="bold red")
        else:
            seg = Text(part.text)
        segments.append(seg)
    return list(Text("\n").join(segments).split("\n"))


def _add_report_node(
    parent,
    node: ReportNode,
    *,
    expand_below_children: int,
    collapsible: bool = True,
    bold: bool = False,
) -> None:
    lines = _rich_lines(node)

    if not collapsible:
        # Flatten this node into *parent*: all of its lines become leaves and its
        # children are added directly to *parent*, so this node cannot be
        # collapsed (there is no node that owns the subtree).
        for line in lines:
            if bold:
                line.stylize("bold")
            parent.add_leaf(line)
        for child in node.children:
            _add_report_node(parent, child, expand_below_children=expand_below_children)
        return

    for line in lines[:-1]:
        parent.add_leaf(line)

    last = lines[-1]
    if node.children:
        tree_node = parent.add(last)
        if len(node.children) < expand_below_children:
            tree_node.expand()
        else:
            tree_node.collapse()
        for child in node.children:
            _add_report_node(
                tree_node, child, expand_below_children=expand_below_children
            )
    else:
        parent.add_leaf(last)


def build_report_tree_widget(
    report_tree: ReportTree,
    *,
    expand_below_children: int = 10,
    collapsible_root: bool = True,
    guide_depth: int = 2,
) -> _Widget:
    """Map *report_tree* onto a ``textual`` ``Tree`` widget.

    Each node's parts are rendered and split into lines: every line but the last
    becomes a leaf, and the last line becomes the node that owns the node's
    children. A node with fewer than ``expand_below_children`` children starts
    expanded, larger ones collapsed. The tree's own root and guide lines are
    hidden.

    Args:
        expand_below_children: Nodes with fewer children start expanded.
        collapsible_root: When ``False`` the report's root node is flattened into
            the top level (its lines become top-level leaves, shown in bold, and
            its children become top-level nodes), so the whole report cannot be
            collapsed away.
        guide_depth: Indentation in cells per tree level (textual clamps this to
            2..10); the default 2 keeps the indentation tight.
    """
    from textual.widgets import Tree

    tree: _Widget = Tree("")
    tree.show_root = False
    tree.show_guides = False
    tree.guide_depth = guide_depth
    _add_report_node(
        tree.root,
        report_tree.root,
        expand_below_children=expand_below_children,
        collapsible=collapsible_root,
        bold=not collapsible_root,
    )
    return tree


def show_report_tree(
    report_tree: ReportTree,
    *,
    title: str | None = None,
    subtitle: str | None = None,
    expand_below_children: int = 10,
    collapsible_root: bool = True,
    guide_depth: int = 2,
) -> bool:
    """Show *report_tree* interactively in a scrollable, collapsible textual view.

    Blocks until the user leaves the view. Nodes are expanded and collapsed
    independently with the usual ``textual`` ``Tree`` keys (Enter / Space / arrow
    keys). See :func:`build_report_tree_widget` for ``collapsible_root`` and
    ``guide_depth``.

    Returns:
        ``True`` if the user confirmed (``c``); ``False`` if they left without
        confirming (``q`` / ``Ctrl-Q`` / ``Esc``).
    """
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.widgets import Footer, Header

    widget = build_report_tree_widget(
        report_tree,
        expand_below_children=expand_below_children,
        collapsible_root=collapsible_root,
        guide_depth=guide_depth,
    )

    class ReportTreeApp(App):
        TITLE = title or "report"
        SUB_TITLE = subtitle or (
            "Enter/Space: expand/collapse · c: confirm · q/Ctrl-Q/Esc: cancel"
        )
        BINDINGS = [
            Binding("c", "confirm", "Confirm"),
            Binding("q", "cancel", "Cancel"),
            Binding("ctrl+q", "cancel", "Cancel"),
            Binding("escape", "cancel", "Cancel"),
        ]

        def compose(self) -> ComposeResult:
            yield Header()
            yield widget
            yield Footer()

        def on_mount(self) -> None:
            widget.focus()

        def action_confirm(self) -> None:
            self.exit(True)

        def action_cancel(self) -> None:
            self.exit(False)

    return bool(ReportTreeApp().run())
