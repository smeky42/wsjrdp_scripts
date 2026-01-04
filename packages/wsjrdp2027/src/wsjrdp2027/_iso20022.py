from __future__ import annotations

import typing as _typing


if _typing.TYPE_CHECKING:
    import io as _io

    import saxonche as _saxonche  # ty: ignore

    from . import _file


_ISO_20022_XML_TO_JSON_XSL = """<?xml version="1.0" encoding="UTF-8"?>
<!--
ISO20022+ Convert XML to JSON.xsl
Version 0.1 Date 2025-03-09
Copyright 2025 ISO20022.plus All rights reserved. Without Warranty.

This stylesheet provides schemaless translation from XML to JSON for ISO 20022 message instances.

Notes.
Only the local names of elements are copied to properties, because JSON Schema does not support namespaces.
Element content is copied as text values, as without schema boolean and numbers can't be distinguished.
The Ccy attributes is a special feature of ISO 20022-4:2013 that indicates an Amount datatypes,
so this is transformed into an object comprising an "amt" and "Ccy".
-->
<xsl:stylesheet
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:xs="http://www.w3.org/2001/XMLSchema"
  xmlns:fn="http://www.w3.org/2005/xpath-functions"
  exclude-result-prefixes="fn"
  version="3.0">
  <xsl:output
    method="text"
    version="1.0"
    encoding="UTF-8"
    indent="no"/>
  <!-- root element -->
  <xsl:template match="/" name="xsl:initial-template">
{
"<xsl:value-of select="fn:local-name(/*)"/>":<xsl:apply-templates select="*"/>
}
  </xsl:template>
  <!-- element with element children -->
  <xsl:template match="*[*]">{<xsl:for-each-group select="*" group-by="local-name()">
"<xsl:value-of select="fn:current-grouping-key()"/>":<xsl:choose>
        <xsl:when test="count(current-group()) = 1">
          <xsl:apply-templates select="."/>
        </xsl:when>
        <xsl:otherwise>[<xsl:for-each select="fn:current-group()">
            <xsl:apply-templates select="."/>
            <xsl:if test="position()!=last()">,</xsl:if>
          </xsl:for-each>]</xsl:otherwise>
      </xsl:choose>
      <xsl:if test="position()!=last()">,</xsl:if>
        </xsl:for-each-group>}</xsl:template>
  <!-- amount element -->
  <xsl:template match="*[not(*)][text()][@Ccy]" priority="10">{"Ccy":"<xsl:value-of select="@Ccy"/>","amt":"<xsl:value-of select="text()"/>"}</xsl:template>
  <!-- leaf element -->
  <xsl:template match="*[not(*)][text()]">"<xsl:value-of select="."/>"</xsl:template>
  <!-- empty element -->
  <xsl:template match="*[not(*)][not(text())]">""</xsl:template>
</xsl:stylesheet>
"""


def _sepa_schema_from_uri(uri: str, /) -> str | None:
    if uri.startswith("urn:iso:std:iso:20022:tech:xsd:"):
        return uri.rsplit(":", 1)[1] or None
    else:
        return None


def _iso20022_sepa_schema_from_xml_document(
    processor: _saxonche.PySaxonProcessor, document: _saxonche.PyXdmNode
) -> str | None:
    xpath_proc = processor.new_xpath_processor()
    xpath_proc.set_context(xdm_item=document)
    ns_items = xpath_proc.evaluate("/*/namespace::*")
    for ns_node in ns_items or []:
        # prefix = ns_node.name  # For namespace nodes, name is the prefix
        uri = ns_node.string_value
        if sepa_schema := _sepa_schema_from_uri(uri):
            return sepa_schema
    else:
        return None


def _iso20022_xml_document_to_dict(
    processor: _saxonche.PySaxonProcessor,
    document: _saxonche.PyXdmNode,
    *,
    expected_format: str | None = None,
) -> dict:
    import json

    sepa_schema = _iso20022_sepa_schema_from_xml_document(processor, document)
    xsltproc = processor.new_xslt30_processor()
    executable = xsltproc.compile_stylesheet(stylesheet_text=_ISO_20022_XML_TO_JSON_XSL)
    output = executable.transform_to_string(xdm_node=document)
    d = json.loads(output)
    if sepa_schema:
        if expected_format:
            if not sepa_schema.startswith(expected_format):
                raise RuntimeError(
                    f"Found format {sepa_schema!r}, expected format {expected_format!r}"
                )
        d["sepa_schema"] = sepa_schema
    elif expected_format:
        raise RuntimeError("Cannot check format as no format was found")

    return d


def iso20022_xml_file_to_dict(
    file_or_path: _file.PathLike | _io.Reader[bytes], /, *, expected_format=None
) -> dict:
    xml_text = slurp_xml_text(file_or_path)
    return iso20022_xml_text_to_dict(xml_text, expected_format=expected_format)


def iso20022_xml_text_to_dict(xml_text: str | bytes, *, expected_format=None) -> dict:
    import saxonche  # ty: ignore

    if isinstance(xml_text, bytes):
        xml_text = decode_xml_bytes(xml_text)
    with saxonche.PySaxonProcessor(license=False) as proc:
        document = proc.parse_xml(xml_text=xml_text)
        return _iso20022_xml_document_to_dict(
            proc, document, expected_format=expected_format
        )


class _Iso20022XmlDetector:
    def __init__(self):
        self.encoding = "utf-8"
        self.sepa_schema = None

    def xml_decl_handler(self, version, encoding, standalone):
        self.encoding = encoding

    def start_namespace(self, prefix, uri):
        if uri.startswith("urn:iso:std:iso:20022:tech:xsd:"):
            self.sepa_schema = uri.rsplit(":", 1)[1]


def decode_xml_bytes(content: bytes, /) -> str:
    encoding = detect_encoding(content)
    return content.decode(encoding=encoding)


def detect_encoding(content: bytes | str) -> str:
    import contextlib
    from xml.parsers import expat

    detector = _Iso20022XmlDetector()
    parser = expat.ParserCreate(namespace_separator=" ")
    parser.XmlDeclHandler = detector.xml_decl_handler
    with contextlib.suppress(expat.ExpatError):
        parser.Parse(content, False)
    return detector.encoding


def slurp_xml_text(path_or_file: _file.PathLike | _io.Reader[bytes]) -> str:
    from . import _file

    xml_bytes = _file.slurp_bytes(path_or_file)
    return decode_xml_bytes(xml_bytes)


def element_or_list_to_list(obj: dict | list | None) -> list:
    if obj is None:
        return []
    elif isinstance(obj, dict):
        return [obj]
    else:
        return obj
