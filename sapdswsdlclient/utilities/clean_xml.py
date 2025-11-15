import xml.etree.ElementTree as ET
import re
import html


def clean_xml_response(xml_string):
    cleaned_xml = xml_string.replace('\xa0', ' ')
    cleaned_xml = ''.join(c for c in cleaned_xml if c.isprintable() or c.isspace())
    cleaned_xml = re.sub(r'&(?!(?:amp|lt|gt|quot|apos);)', '&amp;', cleaned_xml)
    try:
        root = ET.fromstring(cleaned_xml)
        return root
    except ET.ParseError as e:
        raise f'XML parsing error: {e}'


def clean_xml_request(xml_string):
    if not isinstance(xml_string, str):
        return ''

    cdata_marker_base = 'CDATA_ID_{}'
    cdata_map = {}

    def replace_cdata(match):
        index = len(cdata_map)
        cdata_content = match.group(1)
        cdata_map[index] = cdata_content

        return f'<![CDATA[{cdata_marker_base.format(index)}]]>'

    xml_temp = re.sub(r'<!\[CDATA\[(.*?)\]\]>', replace_cdata, xml_string, flags=re.DOTALL)

    cleaned_xml = xml_temp.replace('\xa0', ' ')
    cleaned_xml = html.escape(cleaned_xml, quote=True)
    cleaned_xml = cleaned_xml.replace('\n', ' ').replace('\r', ' ')
    allowed_chars = '\t\r\n'
    cleaned_xml = ''.join(c for c in cleaned_xml if ord(c) >= 32 or c in allowed_chars)
    cleaned_xml = cleaned_xml.replace('\u2028', ' ').replace('\u2029', ' ')
    cleaned_xml = cleaned_xml.replace('"', 'quot;')
    cleaned_xml = cleaned_xml.replace("'", '&apos;')
    cleaned_xml = cleaned_xml.replace(r'\d', r'\\d')


    def get_back_cdata(match):
        marker_full = match.group(1)
        try:
            index = int(marker_full.split('_')[-1])
            return cdata_map[index]
        except (ValueError, IndexError):
            return marker_full

    cleaned_xml = cleaned_xml.replace('&lt;![CDATA[', '<![CDATA[').replace(']]&gt;', ']]>')
    final_xml = re.sub(r'<!\[CDATA\[(.*?)\]\]>', get_back_cdata, cleaned_xml, flags=re.DOTALL)

    return final_xml