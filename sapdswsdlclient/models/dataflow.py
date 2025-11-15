from typing import Literal
import requests
from sapdswsdlclient.templates.templates import request_template, headers
from sapdswsdlclient.utilities.clean_xml import clean_xml_response
from sapdswsdlclient.utilities.check_for_fault_or_error import check_for_fault_or_error
from sapdswsdlclient.server.re_auth import re_logon


class Dataflow:
    def __init__(self, server_instance):
        self._server = server_instance
        self.request_template = request_template
        self.headers = headers

    @re_logon

    def get_df_auditdata(self, repo_name, run_id, dataflow_name):
        """
        :param repo_name: name of the repository
        :param run_id: unique ID of the batch job instance
        :param dataflow_name: name of the dataflow
        :return: audit information for a data flow
        """
        request_body = f'''<ser:Get_DF_AuditdataRequest>
                               <repoName>{repo_name}</repoName>
                               <runID>{run_id}</runID>
                               <dataflow>{dataflow_name}</dataflow>
                            </ser:Get_DF_AuditdataRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_DF_Auditdata'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['ErrorMessage', 'faultstring'])

        audit_data = dict()

        status_code = response.find('.//StatusCode')
        if status_code is not None:
            audit_data['StatusCode'] = status_code.text
        else:
            audit_data['StatusCode'] = None
        audit_point = response.find('.//auditPoint')
        if audit_point is not None:
            audit_data['auditPoint'] = audit_point.text
        else:
            audit_data['auditPoint'] = None
        audit_value = response.find('.//auditValue')
        if audit_value is not None:
            audit_data['auditValue'] = audit_value.text
        else:
            audit_data['auditValue'] = None
        return audit_data


    def get_df_monitor_log(self, repo_name, run_id, dataflow_name, stoponly: Literal['yes', 'no']):
        """
        :param repo_name: name of the repository
        :param run_id: unique ID of the batch job instance
        :param dataflow_name: name of the dataflow
        :param stoponly: if set to yes, the output has only Stop rows (when the object status is in a Stop state)
        :return: runtime statistics for a single data flow execution
        """
        request_body = f'''<ser:Get_DF_Monitor_LogRequest>
                               <repoName>{repo_name}</repoName>
                               <runID>{run_id}</runID>
                               <dataflow>{dataflow_name}</dataflow>
                               <stoponly>{stoponly}</stoponly>
                            </ser:Get_DF_Monitor_LogRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_DF_Monitor_Log'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        df_monitor_data = list()
        root = response.findall('.//Row')
        for row in root:
            row_dict = dict()
            if row is not None:
                row_dict['threadName'] = row.find('threadName').text
                row_dict['state'] = row.find('state').text
                row_dict['absoluteTime'] = row.find('absoluteTime').text
                row_dict['counter'] = row.find('counter').text
                row_dict['rowProcessed'] = row.find('rowProcessed').text
                row_dict['bufferSize'] = row.find('bufferSize').text
                row_dict['bufferUsed'] = row.find('bufferUsed').text
                row_dict['CPUUtilization'] = row.find('CPUUtilization').text
                row_dict['jobServerUsed'] = row.find('jobServerUsed').text
            df_monitor_data.append(row_dict)
        return df_monitor_data