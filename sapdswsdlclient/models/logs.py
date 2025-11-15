from typing import Optional
import requests
from sapdswsdlclient.templates.templates import request_template, headers
from sapdswsdlclient.models.items import MonitorLog, MonitorLogRaw, ErrorLogRaw, TraceLogRaw
from sapdswsdlclient.utilities.clean_xml import clean_xml_response
from sapdswsdlclient.utilities.check_for_fault_or_error import check_for_fault_or_error
from sapdswsdlclient.server.re_auth import re_logon


class Log:
    def __init__(self, server_instance):
        self._server = server_instance
        self.request_template = request_template
        self.headers = headers

    @re_logon

    def get_monitor_log(self, repo_name, run_id, page: Optional[int] = None):
        """
        :param repo_name: name of the repository
        :param run_id: run ID of the batch job instance
        :param page: [Optional] page number of the monitor log
        :return: the monitor log data
        """
        request_body = f'''<ser:Get_Monitor_LogRequest>
                                <repoName>{repo_name}</repoName>
                                <runID>{run_id}</runID>
                                <page>{page}</page>
                            </ser:Get_Monitor_LogRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_Monitor_Log'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['faultstring'])

        monitor_log = dict()

        monitor_log['ReturnCode'] = response.find('.//returnCode').text
        if monitor_log['ReturnCode'] == '1':
            error_message = response.find('.//monitor').text
            raise ValueError(error_message)

        monitor_log_data = response.find('.//monitor').text
        if monitor_log_data is None:
            monitor_log_instance = MonitorLog(None)
            monitor_log_raw_instance = MonitorLogRaw(None)
            monitor_log['MonitorLogMessage'] = monitor_log_instance
            monitor_log['MonitorLogRawMessage'] = monitor_log_raw_instance
            return monitor_log

        monitor_log_message = monitor_log_data.split('\n')
        monitor_log_list = list()
        for row in monitor_log_message:
            if len(row):
                monitor_log_dict = dict()
                row_list = row.split(', ')
                monitor_log_dict['PathName'] = row_list[0]
                monitor_log_dict['State'] = row_list[1]
                monitor_log_dict['RowCount'] = row_list[2]
                monitor_log_dict['ElapsedTime'] = row_list[3]
                monitor_log_dict['AbsoluteTime'] = row_list[4]
                monitor_log_list.append(monitor_log_dict)
        monitor_log_instance = MonitorLog(monitor_log_list)
        monitor_log['MonitorLogMessage'] = monitor_log_instance
        monitor_log_raw_instance = MonitorLogRaw(monitor_log_data)
        monitor_log['MonitorLogMessage'] = monitor_log_instance
        monitor_log['MonitorLogRawMessage'] = monitor_log_raw_instance
        return monitor_log


    def get_error_log(self, repo_name, run_id, page: Optional[int] = None):
        """
        :param repo_name: name of the repository
        :param run_id: run ID of the batch job instance
        :param page: [Optional] page number of the error log
        :return: the error log data
        """
        request_body = f'''<ser:Get_Error_LogRequest>
                                <repoName>{repo_name}</repoName>
                                <runID>{run_id}</runID>
                                <page>{page}</page>
                            </ser:Get_Error_LogRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_Error_Log'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['faultstring'])

        error_log = dict()

        error_log['ReturnCode'] = response.find('.//returnCode').text
        if error_log['ReturnCode'] == '1':
            error_message = response.find('.//error').text
            raise ValueError(error_message)

        error_log_data = response.find('.//error')
        if error_log_data is not None:
            error_log_instance = ErrorLogRaw(error_log_data.text)
        else:
            error_log_instance = ErrorLogRaw(None)

        error_log['errorLogMessage'] = error_log_instance
        return error_log


    def get_trace_log(self, repo_name, run_id, page: Optional[int] = None):
        """
        :param repo_name: name of the repository
        :param run_id: run ID of the batch job instance
        :param page: [Optional] page number of the trace log
        :return: the trace log data
        """
        request_body = f'''<ser:Get_Trace_LogRequest>
                                <repoName>{repo_name}</repoName>
                                <runID>{run_id}</runID>
                                <page>{page}</page>
                            </ser:Get_Trace_LogRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_Trace_Log'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['faultstring'])

        trace_log = dict()

        trace_log['ReturnCode'] = response.find('.//returnCode').text
        if trace_log['ReturnCode'] == '1':
            error_message = response.find('.//trace').text
            raise ValueError(error_message)

        trace_log_data = response.find('.//trace')
        if trace_log_data is not None:
            error_log_instance = TraceLogRaw(trace_log_data.text)
        else:
            error_log_instance = TraceLogRaw(None)

        trace_log['traceLogMessage'] = error_log_instance
        return trace_log