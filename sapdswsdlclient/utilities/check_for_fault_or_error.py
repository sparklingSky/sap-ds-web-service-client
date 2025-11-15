import xml.etree.ElementTree as ET

def check_for_fault_or_error(xml_root: ET.Element, tags: list[str]):
    """
    :param xml_root: xml root after parsing and cleaning
    :param tags: ['errorMessage', 'ErrorMessage', 'faultstring']
    :raises ValueError: in case of error or faultstring
    """
    for tag_name in tags:
        element = xml_root.find(f'.//{tag_name}')

        if element is not None:
            if element.text is not None:
                raise ValueError(element.text)