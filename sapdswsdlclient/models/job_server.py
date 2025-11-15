from sapdswsdlclient.templates.templates import request_template, headers
import requests
from sapdswsdlclient.utilities.clean_xml import clean_xml_response
from sapdswsdlclient.utilities.check_for_fault_or_error import check_for_fault_or_error
from sapdswsdlclient.server.re_auth import re_logon


class JobServer:
    def __init__(self, server_instance):
        self._server = server_instance
        self.request_template = request_template
        self.headers = headers

    @re_logon

    def get_job_server_list(self, repo_name):
        """
        :param repo_name: name of the repository
        :return: name(s) and hostname(s) of the Job Server(s)
        """
        request_body = f'''<ser:GetJobServerListRequest>
                                <repoName>{repo_name}</repoName>
                            </ser:GetJobServerListRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_JobServer_List'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['ErrorMessage', 'faultstring'])

        root = response.findall('.//JobServerInfo')
        job_servers = list()
        for job_server in root:
            if job_server is not None:
                job_servers.append({'jobServerName': job_server.find('jobServerName').text,
                       'jobServerHostname': job_server.find('jobServer').text})
        return job_servers


    def get_job_server_status(self, repo_name, job_server):
        """
        :param repo_name: name of the repository
        :param job_server: job server as <jobserver hostname>:<jobserver port>
        :return: job server status
        """
        request_body = f'''<ser:GetJobServerStatusRequest>
                                <repoName>{repo_name}</repoName>
                                <jobServer>{job_server}</jobServer>
                            </ser:GetJobServerStatusRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_JobServer_Status'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        status = response.find('.//Status').text
        if status == '0':
            return {'statusCode': status, 'statusMessage': 'Job Server is running'}
        elif status == '1':
            return {'statusCode': status, 'statusMessage': 'Job Server can\'t connect to the repository'}
        elif status == '501':
            return {'statusCode': status, 'statusMessage': 'Job Server isn\'t running'}
        else:
            return {'statusCode': status, 'statusMessage': None}


    def get_mc_machine_timezone(self):
        """
        :return: time zone of the Management Console machine
        """
        request_body = f'''<ser:Get_MC_Machine_TimezoneRequest/>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_MC_Machine_Timezone'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        machine_info = dict()
        root = response.find('.//MachineInfo')
        if root is not None:
            machine_info['TimeZone'] = root.find('TimeZone').text
            machine_info['TimeZoneShortName'] = root.find('TimeZoneShortName').text
            machine_info['Offset'] = root.find('Offset').text
        return machine_info