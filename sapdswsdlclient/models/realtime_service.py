import requests
from sapdswsdlclient.templates.templates import request_template, headers
from sapdswsdlclient.utilities.clean_xml import clean_xml_response
from sapdswsdlclient.utilities.check_for_fault_or_error import check_for_fault_or_error
from typing import Literal
from sapdswsdlclient.server.re_auth import re_logon


class RealtimeService:
    def __init__(self, server_instance):
        self._server = server_instance
        self.request_template = request_template
        self.headers = headers

    @re_logon

    def get_as_info(self):
        """
        :return: Access Server information
        """
        request_body = f'''<ser:GetAccessServerInfoRequest/>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'serviceAdmin=Get_AS_Info'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['ErrorMessage', 'faultstring'])

        as_info = dict()
        root = response.find('.//AccessServerInfo')
        if root is not None:
            as_info['AsName'] = root.find('AsName').text
            as_info['MachineName'] = root.find('MachineName').text
            as_info['Port'] = root.find('Port').text
            as_info['UseSSLProtocol'] = root.find('UseSSLProtocol').text
            as_info['Status'] = root.find('Status').text
        return as_info


    def get_rt_msg_format(self, service_name, selector: Literal['in', 'out']):
        """
        :param service_name: name of the real-time service
        :param selector: whether the input or output schema for the service is returned
        :return: the input/output format for a real-time service
        """
        request_body = f'''<ser:Get_RTMsg_FormatRequest>
                                <serviceName>{service_name}</serviceName>
                                <selector>{selector}</selector>
                            </ser:Get_RTMsg_FormatRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'serviceAdmin=Get_RTMsg_Format'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        msg_format_list = list()
        schema = response.find('.//schema')
        if schema is not None:
            msg_format_list.append({'schema': schema.text})
        root_element = response.find('.//rootElement')
        if root_element is not None:
            msg_format_list.append({'rootElement': root_element.text})
        root_element_ns = response.find('.//rootElementNS')
        if root_element_ns is not None:
            msg_format_list.append({'rootElementNS': root_element_ns.text})

        msg_format = dict()
        if selector == 'in':
            msg_format = {'inputFormat': msg_format_list}
        elif selector == 'out':
            msg_format = {'outputFormat': msg_format_list}

        return msg_format


    def get_rt_service_list(self):
        """
        :return: list of the names of published real-time services
        """
        request_body = f'''<ser:Get_RTService_ListRequest/>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'serviceAdmin=Get_RTService_List'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        rt_service_list = list()
        root = response.findall('.//serviceName')
        for element in root:
            rt_service_list.append(element.text)
        return rt_service_list


    def run_rt_service(self, service_name, xml_input):
        """
        :param service_name: name of the realtime service
        :param xml_input: XML input content used to start the real-time service
        :return: error message if there is any and XML output content returned by the realtime service
        """
        request_body = f'''<ser:Run_Realtime_ServiceRequest>
                                <serviceName>{service_name}</serviceName>
                                <xmlInput>{xml_input}</xmlInput>
                            </ser:Run_Realtime_ServiceRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'serviceAdmin=Run_Realtime_Service'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['faultstring'])

        result = dict()
        error_message = response.find('.//errorMessage')
        if error_message is not None:
            result['errorMessage'] = error_message.text
        xml_output = response.find('.//xmlOutput')
        if xml_output is not None:
            result['xmlOutput'] = xml_output.text
        return result