import sapdswsdlclient as ds
import datetime

wsdl_url = 'domain_name:port_number/DataServices/servlet/webservices?ver=2.1'
username = 'your_username'
password = 'your_password'
cms_system = 'hostname'
cms_authentication = 'cms_authentication_type'

start_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
range_start = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
range_start2 = (datetime.datetime.now() - datetime.timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S")
range_end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

repo_name = 'your_repo_name'
job_name = 'job_name'
global_variables = {'var_name1': 'var_value', 'var_name2': 'var_value'}
dataflow = 'dataflow_name'
jobserver = 'jobserver:port'
run_id = '12345'
parameter = 'substitution_parameter'
service_name = 'service_name'

xml_path = 'path_to_your_xml_object'
xml_pass = 'your_passphrase_for_the_object'

server = ds.logon(wsdl_url, username, password, cms_system, cms_authentication)
ds_status = server.ping()
print(ds_status)
print(server.session_id)
print(server.username)
print(repo_name)
print(server.validate_session_id())

job_exe_details  = server.batch_job.get_exe_detail(repo_name, job_name, start_time, end_time)
job_exe_details = server.batch_job.get_exe_detail(repo_name, job_name)
print(job_exe_details)

job_details = server.batch_job.get_detail(repo_name, job_name)
print(job_details)
print(job_details[1].system_configurations_list)
print(job_details[2].substitution_parameters_list)

job_list = server.batch_job.get_list(repo_name)
job_list = server.batch_job.get_list(repo_name, 0)
print(job_list)

jobs_by_time_range = server.batch_job.get_by_time_range(repo_name, range_start, range_end, job_name)
print(jobs_by_time_range)
jobs_by_time_range = server.batch_job.get_by_time_range(repo_name, range_start2, range_end)
print(jobs_by_time_range)

job_flow_details = server.batch_job.get_flow_details(repo_name, run_id)
print(job_flow_details)

job_options = server.batch_job.get_options(job_name, repo_name)
print(job_options)

job_run_exe_details = server.batch_job.get_run_exe_detail(repo_name, job_name, run_id)
print(job_run_exe_details)

job_run_ids = server.batch_job.get_run_ids(repo_name, job_name)
job_run_ids = server.batch_job.get_run_ids(repo_name, job_name, 'warning')
print(job_run_ids)

job_input_format = server.batch_job.get_input_format(repo_name, job_name)
print(job_input_format)

scheduled_tasks = server.batch_job.get_scheduled_tasks(repo_name, all_batch_jobs='true')
scheduled_tasks = server.batch_job.get_scheduled_tasks(repo_name)
print(scheduled_tasks)

run_job = server.batch_job.run_job(repo_name, job_name)
run_job = server.batch_job.run_job(repo_name, job_name, global_variables)
print(run_job)

stop_job = server.batch_job.stop_job(repo_name, run_id)
print(stop_job)

df_audit = server.dataflow.get_df_auditdata(repo_name, run_id, dataflow)
print(df_audit)

df_monitor_log = server.dataflow.get_df_monitor_log(repo_name, run_id, dataflow, stoponly='no')
print(df_monitor_log)

monitor_log = server.log.get_monitor_log(repo_name, run_id)
print(monitor_log)
print(monitor_log['MonitorLogMessage'].monitor_log_list)
print(monitor_log['MonitorLogRawMessage'].monitor_log_raw_data)

error_log = server.log.get_error_log(repo_name, run_id)
print(error_log)
print(error_log['errorLogMessage'].error_log_raw_data)

trace_log = server.log.get_trace_log(repo_name, run_id)
print(trace_log)
print(trace_log['traceLogMessage'].trace_log_raw_data)

job_servers = server.job_server.get_job_server_list(repo_name)
print(job_servers)

job_server_status = server.job_server.get_job_server_status(repo_name, jobserver)
print(job_server_status)

mc_tz = server.job_server.get_mc_machine_timezone()
print(mc_tz)

repo_list = server.repo.get_repo_list()
print(repo_list)

validate_repo_obj = server.repo.validate_repo_object(obj_name=job_name, obj_type='BATCH_JOB', repo_name=repo_name,
                                                     parameter=parameter, trace_on=1)
print(validate_repo_obj)
print(validate_repo_obj['traceMessage'].trace_message)

export_dq_report = server.repo.export_dq_report(repo_name, run_id)
print(export_dq_report)

import_job = server.repo.import_object(repo_name=repo_name, xml_path=xml_path,
                                       passphrase=xml_pass, trace_on=1)
print(import_job)
print(import_job['traceMessage'].trace_message)

as_info = server.realtime_service.get_as_info()
print(as_info)

rt_service_list = server.realtime_service.get_rt_service_list()
print(rt_service_list)

rt_msg_format = server.realtime_service.get_rt_msg_format(service_name, 'out')
print(rt_msg_format)

print(server.logout())