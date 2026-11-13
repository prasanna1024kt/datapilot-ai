"""Source-aware document chunking (V3.1)."""

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

CHUNKING_VERSION = "v3.1"
Span = Dict[str, Any]
Section = Dict[str, Any]
Block = Dict[str, Any]


def _metadata(heading="", parent_heading="", heading_level=0, config_path="", heading_path=None):
    return {
        "section_heading": heading,
        "parent_heading": parent_heading,
        "heading_level": heading_level,
        "heading_path": list(heading_path or []),
        "config_path": config_path,
        "chunking_version": CHUNKING_VERSION,
    }


def _trimmed_range(text: str, start: int):
    left = len(text) - len(text.lstrip())
    right = len(text.rstrip())
    return text[left:right], start + left, start + right


def _spans(start: int, end: int):
    return [{"start_char": start, "end_char": end}]


def _markdown_sections(content: str) -> List[Section]:
    """Parse ATX headings; keep consecutive heading lines with following body."""
    sections = []
    stack = []
    pending_headings = []
    pending_start = None
    body_lines = []
    body_start = None
    position = 0
    meta = _metadata()

    def flush_body():
        nonlocal pending_headings, pending_start, body_lines, body_start
        if body_lines:
            raw = "".join(pending_headings + body_lines)
            start_at = pending_start if pending_start is not None else body_start
            value, start, end = _trimmed_range(raw, start_at)
            if value:
                sections.append({
                    "content": value,
                    "start_char": start,
                    "end_char": end,
                    "source_spans": _spans(start, end),
                    **meta,
                })
        pending_headings = []
        pending_start = None
        body_lines = []
        body_start = None

    for line in content.splitlines(keepends=True):
        clean = line.rstrip("\r\n")
        match = re.match(r"^(#{1,6})[ \t]+(.+?)\s*#*\s*$", clean)
        if match:
            # A heading closes the previous section only if it has body text.
            if body_lines:
                flush_body()
            level = len(match.group(1))
            title = match.group(2).strip()
            stack = [item for item in stack if item[0] < level]
            parent = stack[-1][1] if stack else ""
            stack.append((level, title))
            meta = _metadata(title, parent, level, heading_path=[item[1] for item in stack])
            if pending_start is None:
                pending_start = position
            pending_headings.append(line)
        else:
            if body_start is None:
                body_start = position
            body_lines.append(line)
        position += len(line)

    flush_body()
    # Preserve a trailing heading-only section rather than silently dropping it.
    if pending_headings:
        raw = "".join(pending_headings)
        value, start, end = _trimmed_range(raw, pending_start or 0)
        if value:
            sections.append({
                "content": value,
                "start_char": start,
                "end_char": end,
                "source_spans": _spans(start, end),
                **meta,
            })
    return sections


def _json_top_level_sections(content: str) -> List[Section]:
    """Extract root JSON properties as original source slices with exact offsets."""
    decoder = json.JSONDecoder()
    i = 0
    n = len(content)
    while i < n and content[i].isspace():
        i += 1
    if i >= n or content[i] != "{":
        return []
    i += 1
    sections = []
    while i < n:
        while i < n and (content[i].isspace() or content[i] == ","):
            i += 1
        if i < n and content[i] == "}":
            break
        key_start = i
        try:
            key, key_end = decoder.raw_decode(content, i)
        except json.JSONDecodeError:
            return []
        if not isinstance(key, str):
            return []
        i = key_end
        while i < n and content[i].isspace():
            i += 1
        if i >= n or content[i] != ":":
            return []
        i += 1
        while i < n and content[i].isspace():
            i += 1
        try:
            _, value_end = decoder.raw_decode(content, i)
        except json.JSONDecodeError:
            return []
        raw = content[key_start:value_end]
        value, start, end = _trimmed_range(raw, key_start)
        sections.append({
            "content": value,
            "start_char": start,
            "end_char": end,
            "source_spans": _spans(start, end),
            **_metadata(heading=key, config_path=key),
        })
        i = value_end
    return sections


def _structured_sections(content: str, document_type: str) -> List[Section]:
    kind = document_type.lower()
    if kind == "json":
        try:
            json.loads(content)
        except json.JSONDecodeError:
            pass
        sections = _json_top_level_sections(content)
        if sections:
            return sections
        value, start, end = _trimmed_range(content, 0)
        return [{
            "content": value, "start_char": start, "end_char": end,
            "source_spans": _spans(start, end), **_metadata(),
        }] if value else []

    sections = []
    current_key = "document"
    current_lines = []
    current_start = None
    position = 0

    def flush():
        nonlocal current_lines, current_start
        if current_start is None or not current_lines:
            current_lines, current_start = [], None
            return
        raw = "".join(current_lines)
        value, start, end = _trimmed_range(raw, current_start)
        if value:
            sections.append({
                "content": value, "start_char": start, "end_char": end,
                "source_spans": _spans(start, end),
                **_metadata(heading=current_key, config_path=current_key),
            })
        current_lines, current_start = [], None

    for line in content.splitlines(keepends=True):
        match = re.match(r"^([A-Za-z0-9_.-]+):(?:[ \t].*|[ \t]*)(?:\r?\n)?$", line)
        if match and not line.lstrip().startswith("#"):
            flush()
            current_key = match.group(1)
            current_start = position
            current_lines = [line]
        else:
            if current_start is None:
                current_start = position
            current_lines.append(line)
        position += len(line)
    flush()
    return sections


def _blocks(section: Section) -> List[Block]:
    """Split a section into paragraph, table, and fenced-code blocks.

    Heading-only blocks are not emitted as retrieval content. Their source spans
    are carried into the next content block as heading provenance.
    """
    text = section["content"]
    base = section["start_char"]
    lines = text.splitlines(keepends=True)
    blocks: List[Block] = []
    current: List[str] = []
    current_start = None
    mode = None
    position = base

    def flush():
        nonlocal current, current_start
        if not current or current_start is None:
            current, current_start = [], None
            return
        raw = "".join(current)
        value, start, end = _trimmed_range(raw, current_start)
        if value:
            inherited = _merge_spans(section.get("inherited_heading_spans", []))
            blocks.append({
                **section,
                "content": value,
                "start_char": start,
                "end_char": end,
                "inherited_heading_spans": inherited,
                "source_spans": _merge_spans(inherited + _spans(start, end)),
                "is_table": mode == "table",
                "is_code": mode == "code",
            })
        current, current_start = [], None

    for line in lines:
        stripped = line.strip()
        fence = stripped.startswith(("```", "~~~"))
        if fence:
            if mode == "code":
                current.append(line)
                flush()
                mode = None
            else:
                flush()
                mode = "code"
                current_start = position
                current = [line]
            position += len(line)
            continue
        if mode == "code":
            current.append(line)
            position += len(line)
            continue

        table_line = stripped.startswith("|") or (
            stripped.startswith("-") and "|" in stripped
        )
        if table_line:
            if mode != "table":
                flush()
                mode = "table"
                current_start = position
            current.append(line)
        elif not stripped:
            flush()
            mode = None
        else:
            if mode == "table":
                flush()
            if not current:
                current_start = position
            mode = "paragraph"
            current.append(line)
        position += len(line)
    flush()

    # Headings can occur as standalone blocks inside a section (for example,
    # ## Subsection followed by a fenced code block). Carry their spans forward.
    content_blocks: List[Block] = []
    pending_heading_spans: List[Span] = []
    for block in blocks:
        is_heading_only = bool(
            re.fullmatch(r"#{1,6}[ \t]+.+", block["content"].strip())
        )
        if is_heading_only:
            pending_heading_spans.extend(block.get("source_spans", []))
            continue

        item = dict(block)
        inherited = _merge_spans(
            item.get("inherited_heading_spans", []) + pending_heading_spans
        )
        item["inherited_heading_spans"] = inherited
        item["source_spans"] = _merge_spans(
            inherited + [
                span for span in item.get("source_spans", [])
                if (span["start_char"], span["end_char"])
                == (item["start_char"], item["end_char"])
            ]
        )
        pending_heading_spans = []
        content_blocks.append(item)

    # A trailing heading without content is intentionally not emitted.
    return content_blocks


def _split_table_block(block: Block, chunk_size: int) -> Optional[List[Block]]:
    lines = block["content"].splitlines()
    if len(lines) < 3:
        return None
    header, separator = lines[:2]
    if not (
        header.strip().startswith("|")
        and separator.strip().startswith("|")
        and re.fullmatch(r"\|?[\s:|-]+\|?", separator.strip())
    ):
        return None

    original_lines = block["content"].splitlines(keepends=True)
    cursor = block["start_char"]
    line_spans = []
    for line in original_lines:
        body = line.rstrip("\r\n")
        line_spans.append({"start_char": cursor, "end_char": cursor + len(body)})
        cursor += len(line)

    result = []
    rows = [header, separator]
    spans = [dict(line_spans[0]), dict(line_spans[1])]

    def emit():
        if len(rows) <= 2:
            return
        item = dict(block)
        item.update({
            "content": "\n".join(rows),
            "start_char": min(s["start_char"] for s in spans if not s.get("synthetic")),
            "end_char": max(s["end_char"] for s in spans if not s.get("synthetic")),
            "source_spans": _merge_spans(
                item.get("inherited_heading_spans", []) + spans
            ),
            "is_table": True,
        })
        result.append(item)

    for idx, row in enumerate(lines[2:], start=2):
        candidate = "\n".join(rows + [row])
        if len(candidate) <= chunk_size:
            rows.append(row)
            spans.append(dict(line_spans[idx]))
            continue
        if len(rows) > 2:
            emit()
            rows = [header, separator]
            spans = [
                {**line_spans[0], "synthetic": True},
                {**line_spans[1], "synthetic": True},
            ]
        if len("\n".join(rows + [row])) > chunk_size:
            raise ValueError("A Markdown table row exceeds chunk_size even with its header")
        rows.append(row)
        spans.append(dict(line_spans[idx]))
    emit()
    return result


def _split_block(block: Block, chunk_size: int, chunk_overlap: int) -> List[Block]:
    table_chunks = _split_table_block(block, chunk_size)
    if table_chunks is not None:
        return table_chunks

    text = block["content"]
    if len(text) <= chunk_size:
        return [block]

    result = []
    cursor = 0
    source_start = block["start_char"]
    while cursor < len(text):
        end = min(cursor + chunk_size, len(text))
        if end < len(text):
            boundary = text.rfind("\n", cursor, end)
            if boundary > cursor:
                end = boundary
        if end <= cursor:
            end = min(cursor + chunk_size, len(text))
        raw = text[cursor:end]
        value, start, stop = _trimmed_range(raw, source_start + cursor)
        if value:
            item = dict(block)
            item.update({
                "content": value,
                "start_char": start,
                "end_char": stop,
                "source_spans": _merge_spans(
                    item.get("inherited_heading_spans", []) + _spans(start, stop)
                ),
                "is_table": False,
            })
            result.append(item)
        if end >= len(text):
            break
        cursor = max(cursor + 1, end - chunk_overlap)
    return result

def _merge_heading_only_sections(sections: List[Section]) -> List[Section]:
    """Carry heading-only section provenance into the next content section."""
    result = []
    pending_spans = []

    for section in sections:
        is_heading_only = bool(
            re.fullmatch(
                r"#{1,6}[ \t]+.+",
                section["content"].strip(),
            )
        )

        if is_heading_only:
            pending_spans.extend(section.get("source_spans", []))
            continue

        item = dict(section)
        item["inherited_heading_spans"] = _merge_spans(
            pending_spans + item.get("inherited_heading_spans", [])
        )
        pending_spans = []
        result.append(item)

    # A trailing heading without content is not emitted as a vector.
    return result


def _merge_spans(spans: Iterable[Span]) -> List[Span]:
    result = []
    seen = set()
    for span in spans:
        key = (span["start_char"], span["end_char"], bool(span.get("synthetic")))
        if key not in seen:
            seen.add(key)
            result.append(dict(span))
    return result


def chunk_document(
    document: Dict[str, Any],
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> List[Dict[str, Any]]:
    content = document.get("content", "")
    if not isinstance(content, str):
        raise TypeError("Document content must be a string")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if chunk_overlap < 0 or chunk_overlap >= chunk_size:
        raise ValueError("chunk_overlap must be non-negative and smaller than chunk_size")

    required = ("document_id", "source_file", "document_type")
    missing = [field for field in required if field not in document]
    if missing:
        raise ValueError(f"Missing required document fields: {missing}")
    if not content.strip():
        return []

    kind = str(document["document_type"]).lower()
    if kind == "md":
        sections = _markdown_sections(content)
    elif kind in {"yaml", "yml", "json"}:
        sections = _structured_sections(content, kind)
    else:
        value, start, end = _trimmed_range(content, 0)
        sections = [{
            "content": value, "start_char": start, "end_char": end,
            "source_spans": _spans(start, end), **_metadata(),
        }] if value else []

    blocks = []
    for section in _merge_heading_only_sections(sections):
        for block in _blocks(section):
            heading_path = block.get("heading_path", [])
            prefix = (
                f"[Section: {' > '.join(heading_path)}]\n"
                if kind == "md" and heading_path
                else ""
            )
            available_size = chunk_size - len(prefix)
            if available_size <= 0:
                raise ValueError(
                    "Heading context exceeds chunk_size"
                )
            effective_overlap = min(
                chunk_overlap,
                max(0, available_size - 1),
            )
            pieces = _split_block(
                block,
                available_size,
                effective_overlap,
            )
            for piece in pieces:
                piece["content"] = prefix + piece["content"]
                piece["context_prefix_length"] = len(prefix)
                blocks.append(piece)

    packed = []
    current = None
    for block in blocks:
        same_section = (
            current is not None
            and current["section_heading"] == block["section_heading"]
            and current["parent_heading"] == block["parent_heading"]
            and current.get("heading_path", []) == block.get("heading_path", [])
            and current["config_path"] == block["config_path"]
        )
        prefix_length = block.get("context_prefix_length", 0)
        current_prefix_length = (
            current.get("context_prefix_length", 0)
            if current is not None else 0
        )

        same_prefix = (
            current is not None
            and current["content"][:current_prefix_length]
            == block["content"][:prefix_length]
        )

        combined = (
            current["content"]
            + "\n\n"
            + block["content"][prefix_length:]
            if same_section and same_prefix
            else block["content"]
        )
        if (
            current is not None and same_section
            and not current["contains_table"]
            and not block.get("is_table", False)
            and not current.get("is_code", False)
            and not block.get("is_code", False)
            and len(combined) <= chunk_size
        ):
            current["content"] = combined
            current["source_spans"] = _merge_spans(
                current["source_spans"] + block.get("source_spans", [])
            )
            current["end_char"] = max(current["end_char"], block.get("end_char", 0))
        else:
            if current is not None:
                packed.append(current)
            current = {
                **block,
                "contains_table": bool(block.get("is_table", False)),
                "is_code": bool(block.get("is_code", False)),
                "source_spans": _merge_spans(block.get("source_spans", [])),
            }
    if current is not None:
        packed.append(current)

    results = []
    for index, chunk in enumerate(packed):
        spans = _merge_spans(chunk.get("source_spans", []))
        real = [s for s in spans if not s.get("synthetic", False)]
        start_char = min((s["start_char"] for s in real), default=chunk["start_char"])
        end_char = max((s["end_char"] for s in real), default=chunk["end_char"])
        chunk_id = hashlib.sha256(
            f"{document['document_id']}:{CHUNKING_VERSION}:{index}:{chunk['content']}".encode("utf-8")
        ).hexdigest()
        results.append({
            "chunk_id": chunk_id,
            "document_id": document["document_id"],
            "source_file": document["source_file"],
            "document_type": document["document_type"],
            "chunk_index": index,
            "start_char": start_char,
            "end_char": end_char,
            "section_heading": chunk["section_heading"],
            "parent_heading": chunk["parent_heading"],
            "heading_level": chunk["heading_level"],
            "heading_path": chunk.get("heading_path", []),
            "config_path": chunk["config_path"],
            "chunking_version": CHUNKING_VERSION,
            "content": chunk["content"],
            "is_table": chunk.get("contains_table", False),
            "is_code": chunk.get("is_code", False),
            "source_spans": spans,
        })
    return results
