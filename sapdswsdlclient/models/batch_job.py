from typing import Optional, Literal
import requests
import html
import re
from sapdswsdlclient.templates.templates import request_template, headers
from sapdswsdlclient.models.items import SystemConfigurations, SubstitutionParameters
from sapdswsdlclient.utilities.clean_xml import clean_xml_response
from sapdswsdlclient.utilities.check_for_fault_or_error import check_for_fault_or_error
from sapdswsdlclient.server.re_auth import re_logon


class BatchJob:
    def __init__(self, server_instance):
        """
        :param server_instance:
        """
        self._server = server_instance
        self.request_template = request_template
        self.headers = headers

    @re_logon

    def get_exe_detail(self, repo_name, job_name, start_time: Optional[str] = '', end_time: Optional[str] = ''):
        """
        :param repo_name: name of the repository
        :param job_name: name of the batch job
        :param start_time: [Optional] date and time that a job has started running as YYYY-MM-DD HH:mm:ss
        :param end_time: [Optional] ending date and time of the start time range as YYYY-MM-DD HH:mm:ss
        :return: a list of all job executions for a selected repository and job
        """

        request_body = f'''<ser:GetBatchJobExeDetailRequest>
                            <repoName>{repo_name}</repoName>
                            <jobName>{job_name}</jobName>
                            <startTime>{start_time}</startTime>
                            <endTime>{end_time}</endTime>
                        </ser:GetBatchJobExeDetailRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_BatchJob_ExeDetail'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)
        job_exe_details = list()

        root = response.findall('.//jobDetail')
        if root:
            for job in root:
                job_item = dict()
                job_item['JobName'] = job.find('JobName').text
                job_item['ObjID'] = job.find('ObjID').text
                job_item['RunID'] = job.find('runID').text
                job_item['StartTime'] = job.find('StartTime').text
                job_item['EndTime'] = job.find('EndTime').text
                job_item['ExecutionTime'] = job.find('ExecutionTime').text
                job_item['Status'] = job.find('Status').text
                job_item['JobServerUsed'] = job.find('JobServerUsed').text
                job_exe_details.append(job_item)
            return job_exe_details
        else:
            return {'returnCode': '0', 'returnMessage': 'No job execution details found'}


    def get_detail(self, repo_name, job_name):
        """
        :param repo_name: name of the repository
        :param job_name: name of the batch job
        :return: a list of a job's global variables, all the available system configurations
                and substitution parameters
        """
        request_body = f'''<ser:Get_BatchJob_DetailRequest>
                                <jobName>{job_name}</jobName>
                                <repoName>{repo_name}</repoName>
                        </ser:Get_BatchJob_DetailRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_BatchJob_Details'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['ErrorMessage', 'faultstring'])

        job_details = list()

        global_variables = response.find('.//globalVariables')
        system_configurations = response.find('.//systemConfigurations')
        substitution_parameters = response.find('.//substitutionParameters')

        if global_variables is not None:
            var_list = list()
            for var in global_variables:
                var_dict = dict()
                if var is not None:
                    var_dict['name'] = var.get('name')
                    var_dict['type'] = var.get('type')
                    var_dict['default_value'] =  var.text
                    var_list.append(var_dict)
            job_details.append({'globalVariables': var_list})

        if system_configurations is not None:
            config_list = list()
            for conf in system_configurations:
                if conf is not None:
                    config_list.append(conf.get('name'))
            system_configurations_instance = SystemConfigurations(config_list)
            job_details.append(system_configurations_instance)

        if substitution_parameters is not None:
            param_list = list()
            for param in substitution_parameters:
                if param is not None:
                    param_list.append({param.get('name'): param.text})
            substitution_parameters_instance = SubstitutionParameters(param_list)
            job_details.append(substitution_parameters_instance)

            return job_details


    def get_by_time_range(self, repo_name, range_start_time, range_end_time, job_name: Optional[str] = ''):
        """
        :param repo_name: name of the repository
        :param range_start_time: earliest datetime of the range as YYYY-MM-DD HH:mm:ss
        :param range_end_time: latest datetime of the range as YYYY-MM-DD HH:mm:ss
        :param job_name: [Optional] the name of the batch job
        :return: a list of all jobs or all instances of the specified job running during the specified time range
        """
        request_body = f'''<ser:Get_BatchJob_By_TimeRangeRequest>
                                <repoName>{repo_name}</repoName>
                                <jobName>{job_name}</jobName>
                                <rangeStartTime>{range_start_time}</rangeStartTime>
                                <rangeEndTime>{range_end_time}</rangeEndTime>
                            </ser:Get_BatchJob_By_TimeRangeRequest>'''
        self.headers['SOAPAction'] = 'jobAdmin=Get_BatchJob_By_TimeRange'
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)
        jobs_by_time_range = list()

        root = response.findall('.//jobDetail')
        if root:
            for job in root:
                job_item = dict()
                job_item['JobName'] = job.find('JobName').text
                job_item['ObjID'] = job.find('ObjID').text
                job_item['RunID'] = job.find('runID').text
                job_item['StartTime'] = job.find('StartTime').text
                job_item['EndTime'] = job.find('EndTime').text
                job_item['ExecutionTime'] = job.find('ExecutionTime').text
                job_item['Status'] = job.find('Status').text
                job_item['JobServerUsed'] = job.find('JobServerUsed').text
                jobs_by_time_range.append(job_item)
            return jobs_by_time_range
        else:
            return f'No jobs found in the time range {range_start_time} - {range_end_time}.'


    def get_list(self, repo_name, is_all_batch_jobs: Literal[0, 1] = 1):
        """
        :param repo_name: name of the repository
        :param is_all_batch_jobs: [Optional] 0 if the jobs published as Web services; 1 if all the jobs
        :return: the list of batch jobs
        """
        request_body = f'''<ser:Get_BatchJob_ListRequest>
                                <repoName>{repo_name}</repoName>
                                <allBatchJobs>{is_all_batch_jobs}</allBatchJobs>
                            </ser:Get_BatchJob_ListRequest>'''
        self.headers['SOAPAction'] = 'jobAdmin=Get_BatchJob_List'
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        root = response.findall('.//jobName')
        job_list = list()
        for job in root:
            if job is not None:
                job_list.append(job.text)
        return {repo_name: job_list}


    def get_flow_details(self, repo_name, run_id):
        """
        :param repo_name: name of the repository
        :param run_id: run ID of the batch job instance
        :return: the batch job instance objects execution statistics
        """
        request_body = f'''<ser:Get_BatchJob_FlowDetailsRequest>
                                <repoName>{repo_name}</repoName>
                                <runID>{run_id}</runID>
                            </ser:Get_BatchJob_FlowDetailsRequest>'''
        self.headers['SOAPAction'] = 'jobAdmin=Get_BatchJob_FlowDetails'
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['ErrorMessage', 'faultstring'])

        flow_details = list()

        root = response.findall('.//flowDetail')
        if root:
            for flow in root:
                flow_item = dict()
                flow_item['ObjectName'] = flow.find('ObjectName').text
                flow_item['ObjectType'] = flow.find('ObjectType').text
                flow_item['ParentObject'] = flow.find('ParentObject').text
                flow_item['ParentType'] = flow.find('ParentType').text
                flow_item['StartTime'] = flow.find('StartTime').text
                flow_item['EndTime'] = flow.find('EndTime').text
                flow_item['Duration'] = flow.find('Duration').text
                flow_item['RowsRead'] = flow.find('RowsRead').text
                flow_item['JobServerUsed'] = flow.find('JobserverUsed').text
                flow_item['hasAuditData'] = flow.find('hasAuditData').text
                flow_details.append(flow_item)
            return flow_details


    def get_options(self, job_name, repo_name):
        """
        :param job_name: name of the job
        :param repo_name: name of the repository
        :return: dict of job execution and trace options
        """
        request_body = f'''<ser:Get_BatchJob_OptionsRequest>
                                <jobName>{job_name}</jobName>
                                <repoName>{repo_name}</repoName>
                            </ser:Get_BatchJob_OptionsRequest>'''
        self.headers['SOAPAction'] = 'jobAdmin=Get_BatchJob_Options'
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['ErrorMessage', 'faultstring'])

        job_options = dict()
        job_options['sampling_rate'] = response.find('.//sampling_rate').text
        job_options['auditing'] = response.find('.//auditing').text
        job_options['disableValidationStatisticsCollection'] = (
            response.find('.//disableValidationStatisticsCollection').text)
        job_options['recovery'] = response.find('.//recovery').text
        job_options['recoverfromlastfailedexec'] = (
            response.find('.//recoverfromlastfailedexec').text)
        job_options['collectstatsformonitoring'] = (
            response.find('.//collectstatsformonitoring').text)
        job_options['collectstatsforoptimization'] = (
            response.find('.//collectstatsforoptimization').text)
        job_options['usecollectedstats'] = response.find('.//usecollectedstats').text
        job_options['exportdataqualityreports'] = (
            response.find('.//exportdataqualityreports').text)
        job_options['trace'] = list()
        trace_options = response.findall('.//trace')
        for trace in trace_options:
            if trace is not None:
                job_options['trace'].append(trace.text)
        job_options['StatusCode'] = response.find('.//StatusCode').text

        return job_options


    def get_run_exe_detail(self, repo_name, job_name, run_id):
        """
        :param job_name: name of the job
        :param repo_name: name of the repository
        :param run_id: run ID of the job instance
        :return: dict of details for the job instance execution
        """
        request_body = f'''<ser:Get_BatchJob_Run_ExeDetailRequest>
                                <repoName>{repo_name}</repoName>
                                <jobName>{job_name}</jobName>
                                <runID>{run_id}</runID>
                            </ser:Get_BatchJob_Run_ExeDetailRequest>'''

        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_BatchJob_Run_ExeDetail'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)
        job_run_exe_details = dict()

        root = response.find('.//jobDetail')
        if root is not None:
            job_run_exe_details['ObjID'] = root.find('ObjID').text
            job_run_exe_details['StartTime'] = root.find('StartTime').text
            job_run_exe_details['EndTime'] = root.find('EndTime').text
            job_run_exe_details['ExecutionTime'] = root.find('ExecutionTime').text
            job_run_exe_details['Status'] = root.find('Status').text
            job_run_exe_details['JobServerUsed'] = root.find('JobServerUsed').text

        else:
            raise ValueError(f'No execution details for run ID {run_id} of {job_name} in {repo_name}.')

        return job_run_exe_details


    def get_run_ids(self, repo_name, job_name, status: Optional[str] = 'all'):
        """
        :param repo_name: name of the repository
        :param job_name: name of the job
        :param status: the options are running, succeeded, error, warning, all
        :return: list of run IDs for batch job instances
        """
        request_body = f'''<ser:Get_BatchJob_RunIDsRequest>
                                <repoName>{repo_name}</repoName>
                                <jobName>{job_name}</jobName>
                                <status>{status}</status>
                            </ser:Get_BatchJob_RunIDsRequest>'''

        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_BatchJob_RunIDs'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        runs = list()

        runs_data = response.findall('.//run')
        for run in runs_data:
            run_dict = dict()
            if run is not None:
                run_dict['runID'] = run.find('runID').text
                run_dict['status'] = run.find('status').text
                run_dict['repoName'] = run.find('repoName').text
            runs.append(run_dict)
        return runs


    def get_input_format(self, repo_name, job_name):
        """
        :param repo_name: name of the repository
        :param job_name: name of the job
        :return: the input format for a batch job
        """
        request_body = f'''<ser:Get_Job_Input_FormatRequest>
                                <repoName>{repo_name}</repoName>
                                <jobName>{job_name}</jobName>
                            </ser:Get_Job_Input_FormatRequest>'''

        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_Job_Input_Format'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        input_format = dict()
        job_input = None
        var_list = list()

        xml_namespaces = {'localtypes': 'http://www.businessobjects.com/DataServices/ServerX.xsd'}
        xml_root = response
        format_element = xml_root.find('.//format', xml_namespaces).text

        xsd_string = html.unescape(format_element)
        xsd_lines = xsd_string.split('xsd:element')
        for line in xsd_lines:
            job_match = re.search(r"name='(.*?_GlobalVariables)'", line)
            if job_match:
                job_input = job_match.group(1)
                xsd_lines.remove(line)

        for line in xsd_lines:
            var_dict = dict()
            var_name_match = re.search(r"name='(.*?)'", line)
            if var_name_match:
                var_dict['name'] = var_name_match.group(1)

            var_type_match = re.search(r"data type '(.*?)'", line)
            if var_type_match:
                var_size_match = re.search(r"with size=(.*?)--", line)
                if var_size_match:
                    var_dict['dataType'] = f'{var_type_match.group(1)}({var_size_match.group(1)})'
                else:
                    var_dict['dataType'] = var_type_match.group(1)

            if var_dict:
                var_list.append(var_dict)
        input_format[job_input] = var_list
        return input_format


    def get_scheduled_tasks(self, repo_name, all_batch_jobs: Optional[str] = 'false'):
        """
        :param repo_name: name of the repository
        :param all_batch_jobs: true (all) / false (active)
        :return: list of scheduled tasks
        """
        request_body = f'''<ser:Get_Scheduled_TasksRequest>
                                <repoName>{repo_name}</repoName>
                                <allBatchJobs>{all_batch_jobs}</allBatchJobs>
                            </ser:Get_Scheduled_TasksRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Get_Scheduled_Tasks'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        scheduled_tasks = list()
        tasks = response.findall('.//SchduledTask')
        for t in tasks:
            task_description = dict()
            if t is not None:
                task_description['ScheduledTaskName'] = t.find('ScheduledTaskName').text
                task_description['JobName'] = t.find('JobName').text
                task_description['RepoName'] = t.find('RepoName').text
                task_description['RecurrenceType'] = t.find('RecurrenceType').text
                task_description['RecurrenceDays'] = t.find('RecurrenceDays').text
                task_description['StartTime'] = t.find('StartTime').text
                task_description['DurationTime'] = t.find('DurationTime').text
                task_description['RepeatInterval'] = t.find('RepeatInterval').text
                task_description['NextRunTime'] = t.find('NextRunTime').text
                task_description['IsActive'] = t.find('IsActive').text
            scheduled_tasks.append(task_description)
        return scheduled_tasks


    def run_job(self, repo_name, job_name,
                job_parameters: Optional[str] = '', global_variables:  Optional[dict] = '',
                job_server: Optional[str] = '', server_group: Optional[str] = ''):
        """
        :param repo_name: name of the repository
        :param job_name: name of the job
        :param job_parameters: [Optional] XML element that sets specific job execution parameters
        :param global_variables: [Optional] dict in format {'var_name': 'var_value'}
        :param job_server: [Optional] name of the job server to execute the job;
                            can't be specified if serverGroup is also specified.
        :param server_group: [Optional] name of the server group to use to execute the job;
                            can't be specified if jobServer is specified
        :return: process ID, counter ID, run ID, repository name associated with the batch job execution
        """
        if job_server and server_group:
            raise ValueError('You can only specify either Job Server or Server Group, not both.')

        global_variables_xml = ''
        if global_variables:
            existing_vars = list(self.get_input_format(repo_name, job_name).values())[0]
            existing_vars_list = list()
            for var in existing_vars:
                for name, value in var.items():
                    if name == 'name':
                        existing_vars_list.append(value)

            for name, value in global_variables.items():
                if name in existing_vars_list:
                    global_variables_xml += f'''<variable name="{name}">'{value}'</variable>'''
                else:
                    raise ValueError(f'''No '{name}' variable existing for {job_name}''')

        request_body = f'''<ser:Run_Batch_JobRequest>
                                <repoName>{repo_name}</repoName>
                                <jobName>{job_name}</jobName>
                                <jobParameters>{job_parameters}</jobParameters>
                                <globalVariables>
                                    {global_variables_xml}
                                </globalVariables>
                                <jobServer>{job_server}</jobServer>
                                <serverGroup>{server_group}</serverGroup>
                            </ser:Run_Batch_JobRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Run_Batch_Job'
        response = requests.post(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        run_status = dict()
        run_status['processID'] = response.find('.//pid').text
        run_status['counterID'] = response.find('.//cid').text
        run_status['runID'] = response.find('.//rid').text
        run_status['repoName'] = response.find('.//repoName').text
        return run_status


    def stop_job(self, repo_name, run_id):
        """
        :param repo_name: name of the repository
        :param run_id: run ID of the bath job instance
        :return: status of the operation
        """
        request_body = f'''<ser:Run_Batch_JobRequest>
                                <repoName>{repo_name}</repoName>
                                <runID>{run_id}</runID>
                            </ser:Run_Batch_JobRequest>'''
        request = self.request_template.format(session_id=self._server.session_id, request_body=request_body)
        self.headers['SOAPAction'] = 'jobAdmin=Stop_Batch_Job'
        response = requests.get(self._server.wsdl_url, data=request, headers=self.headers)

        response = clean_xml_response(response.text)

        check_for_fault_or_error(response, ['errorMessage', 'faultstring'])

        status = dict()
        return_code = response.find('.//returnCode')
        if return_code is not None:
            status['returnCode'] = return_code.text
            if return_code.text == '0':
                status['returnMessage'] = 'The operation successfully stopped the specified batch job.'
            if return_code.text == '1':
                status['returnMessage'] = 'The operation failed to stop the specified batch job in the specified repository.'
        return status