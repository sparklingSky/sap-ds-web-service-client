from typing import Optional, Literal
import requests
from sapdswsdlclient.templates.templates import request_template, headers
from sapdswsdlclient.models.items import TraceMessage, ErrorMessage
from sapdswsdlclient.utilities.clean_xml import clean_xml_response, clean_xml_request
from sapdswsdlclient.utilities.check_for_fault_or_error import check_for_fault_or_error
from sapdswsdlclient.server.re_auth import re_logon


class Repo:
    def __init__(self, server_instance):
        self._server = server_instance
        self.request_template = request_template
        self.headers = headers

    @re_logon

    def get_repo_list(self):
        """
        :return: list of the repositories with their attributes available for the authenticated user
        """
        request_body = f'''<ser:Get_Repository_ListRequest/>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'repoAdmin=Get_Repository_List'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        repo_list = list()
        root = response.findall('.//repository')
        if root is not None:
            for repo in root:
                repo_attr = dict()
                repo_attr['repoName'] = repo.find('repoName').text
                repo_attr['repoType'] = repo.find('repoType').text
                repo_attr['dbType'] = repo.find('dbType').text
                repo_attr['dbHost'] = repo.find('dbHost').text
                repo_attr['connectionStatus'] = repo.find('connectionStatus').text
                repo_attr['username'] = repo.find('username').text
                repo_attr['permissions'] = repo.find('permissions').text
                repo_list.append(repo_attr)
        return repo_list


    def validate_repo_object(self, obj_name, obj_type, repo_name, parameter, system_profile: Optional[str] = '',
                             job_server: Optional[str] = '', server_group: Optional[str] = '',
                             substitution_parameters: Optional[complex] = '', trace_on: Literal[0, 1] = ''):
        """
        :param obj_name: name of the object to validate
        :param obj_type: BATCH_JOB / REALTIME_JOB / WORKFLOW / DATAFLOW / ABAP_DATAFLOW /
                        DATA_QUALITY_TRANSFORM_CONFIGURATION / CUSTOM_FUNCTION
        :param repo_name: name of the repository
        :param parameter: individual substitution parameter
        :param system_profile: [Optional] name of the job system profile to use while validating the object
        :param job_server: [Optional] name of the job server associated with the repository;
                            can't be specified if serverGroup is also specified
        :param server_group: [Optional] name of the server group associated with the repository;
                            can't be specified if jobServer is also specified.
        :param substitution_parameters: [Optional] substitution parameters to override while validating the object
        :param trace_on: [Optional] enables tracing for the operation
        :return: validation status of the object and optionally trace log
        """
        if job_server and server_group:
            raise ValueError('You can only specify either Job Server or Server Group, not both.')

        request_body = f'''<ser:Validate_Repo_ObjectRequest>
                                <objName>{obj_name}</objName>
                                <objType>{obj_type}</objType>
                                <repoName>{repo_name}</repoName>
                                <systemProfile>{system_profile}</systemProfile>
                                <jobServer>{job_server}</jobServer>
                                <serverGroup>{server_group}</serverGroup>
                                <substitutionParameters>{substitution_parameters}</substitutionParameters>    
                                <parameter>{parameter}</parameter>
                                <traceOn>{trace_on}</traceOn>
                        </ser:Validate_Repo_ObjectRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'repoAdmin=Validate_Repo_Object'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['faultstring'])

        validated_object = dict()
        return_code = response.find('.//returnCode').text
        validated_object['returnCode'] = return_code

        if return_code == '0':
            validated_object['returnMessage'] = 'The operation completed successfully.'
        elif return_code == '1':
            validated_object['returnMessage'] = 'The operation failed to complete successfully.'

        error_message = response.find('.//errorMessage')
        if error_message is not None:
            error_instance = ErrorMessage(error_message.text)
        else:
            error_instance = ErrorMessage(None)
        validated_object['errorMessage'] = error_instance

        trace_message = response.find('.//traceMessage')
        if trace_message is not None:
            trace_instance = TraceMessage(trace_message.text)
        else:
            trace_instance = TraceMessage(None)
        validated_object['traceMessage'] = trace_instance

        return validated_object


    def delete_repo_object(self, obj_name, obj_type, repo_name, job_server: Optional[str] = '',
                           server_group: Optional[str] = '', trace_on: Literal[0, 1] = ''):
        """
        :param obj_name: name of the object to validate
        :param obj_type: BATCH_JOB / REALTIME_JOB / WORKFLOW / DATAFLOW / ABAP_DATAFLOW /
                        DATA_QUALITY_TRANSFORM_CONFIGURATION / CUSTOM_FUNCTION
        :param repo_name: name of the repository
        :param job_server: [Optional] name of the job server associated with the repository;
                            can't be specified if serverGroup is also specified
        :param server_group: [Optional] name of the server group associated with the repository;
                            can't be specified if jobServer is also specified.
        :param trace_on: [Optional] if 1, enables tracing for the operation
        :return: status of the attempt to delete the object and optionally trace log
        """
        if job_server and server_group:
            raise ValueError('You can only specify either Job Server or Server Group, not both.')

        request_body = f'''<ser:Delete_Repo_ObjectsRequest>
                                <objName objType='{obj_type}'>{obj_name}</objName>
                                <repoName>{repo_name}</repoName>
                                <jobServer>{job_server}</jobServer>
                                <serverGroup>{server_group}</serverGroup>
                                <traceOn>{trace_on}</traceOn>
                            </ser:Delete_Repo_ObjectsRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'repoAdmin=Delete_Repo_Objects'
        response = requests.post(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['faultstring'])

        result = dict()
        result['returnCode'] = response.find('.//returnCode').text

        if result['returnCode'] == '0':
            result['returnMessage'] = 'The operation completed successfully'
        elif result['returnCode'] == '1':
            result['returnMessage'] = 'The operation failed to complete successfully.'

        error_message = response.find('.//errorMessage')
        if error_message is not None:
            error_instance = ErrorMessage(error_message.text)
        else:
            error_instance = ErrorMessage(None)
        result['errorMessage'] = error_instance

        trace_message = response.find('.//traceMessage')
        if trace_message is not None:
            deleted_repo_object_instance = TraceMessage(trace_message.text)
        else:
            deleted_repo_object_instance = TraceMessage(None)
        result['traceMessage'] = deleted_repo_object_instance

        return result


    def export_dq_report(self, repo_name, run_id):
        """
        :param repo_name: name of the repository
        :param run_id: run ID of the batch job instance
        :return: information about the exported data quality report
        """
        request_body = f'''<ser:Export_DQReportRequest>
                                <repoName>{repo_name}</repoName>
                                <runID>{run_id}</runID>
                            </ser:Export_DQReportRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'repoAdmin=Export_DQReport'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['faultstring'])

        exported = dict()
        export_status = response.find('.//exportStatus')
        if export_status is not None:
            exported['exportStatus'] = export_status.text
        status_message = response.find('.//statusMessage')
        if status_message is not None:
            exported['statusMessage'] = status_message.text
        process_message = response.find('.//processMessage')
        if process_message is not None:
            exported['processMessage'] = process_message.text
        export_path = response.find('.//exportPath')
        if export_path is not None:
            exported['exportPath'] = export_path.text
        export_file_name = response.find('.//exportFileName')
        if export_file_name is not None:
            exported['exportFileName'] = export_file_name.text
        report_name = response.find('.//reportName')
        if report_name is not None:
            exported['reportName'] = report_name.text
        report_status = response.find('.//reportStatus')
        if report_status is not None:
            exported['reportStatus'] = report_status.text
        return exported


    def import_object(self, repo_name, xml_path, passphrase, trace_on: Literal[0, 1] = '',
                      job_server: Optional[str] = '', server_group: Optional[str] = ''):
        """
        :param repo_name: name of the repository
        :param xml_path: path to the xml file of the object to import
        :param passphrase: passphrase for the xml file
        :param trace_on: [Optional] if 1, enables tracing for the operation
        :param job_server: [Optional] name of the job server associated with the repository;
                            can't be specified if serverGroup is also specified
        :param server_group: [Optional] name of the server group associated with the repository;
                            can't be specified if jobServer is also specified.
        :return: status of the operation to import the object
        """
        if job_server and server_group:
            raise ValueError('You can only specify either Job Server or Server Group, not both.')

        with open(xml_path, 'r') as f:
            definition = f.read()
        if definition.startswith('<?xml'):
            end_index = definition.find('?>')
            if end_index != -1:
                definition = definition[end_index + 2:].lstrip()

        definition = clean_xml_request(definition)
        request_body = f'''<ser:ImportObjectDefinitionRequest>
                            <repoName>{repo_name}</repoName>
                            <definition>{definition}</definition>
                            <jobServer>{job_server}</jobServer>
                            <serverGroup>{server_group}</serverGroup>
                            <passphrase>{passphrase}</passphrase>
                            <traceOn>{trace_on}</traceOn>
                        </ser:ImportObjectDefinitionRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)

        encoded_data = request.encode('utf-8-sig')
        self.headers['SOAPAction'] = 'repoAdmin=Import_Repo_Object'
        response = requests.post(self._server.wsdl_url, data=encoded_data, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['faultstring'])

        imported_object = dict()
        return_code = response.find('.//returnCode')
        if return_code is not None:
            imported_object['returnCode'] = return_code.text
            if return_code.text == '0':
                imported_object['returnMessage'] = 'The operation successfully imported the object.'
            if return_code.text == '1':
                imported_object['returnMessage'] = 'The operation failed to import the object.'

        error_message = response.find('.//errorMessage')
        if error_message is not None:
            error_instance = ErrorMessage(error_message.text)
        else:
            error_instance = ErrorMessage(None)
        imported_object['errorMessage'] = error_instance

        trace_message = response.find('.//traceMessage')
        if trace_message is not None:
            trace_instance = TraceMessage(trace_message.text)
        else:
            trace_instance = TraceMessage(None)
        imported_object['traceMessage'] = trace_instance

        return imported_object