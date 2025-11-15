class SystemConfigurations:
    def __init__(self, system_configurations_list):
        self.system_configurations_list = system_configurations_list

    def __repr__(self):
        return '<SystemConfigurationsItem>'


class SubstitutionParameters:
    def __init__(self, substitution_parameters_list):
        self.substitution_parameters_list = substitution_parameters_list

    def __repr__(self):
        return '<SubstitutionParametersItem>'


class MonitorLog:
    def __init__(self, monitor_log_list):
        self.monitor_log_list = monitor_log_list

    def __repr__(self):
        if self.monitor_log_list is None:
            return 'None'
        else:
            return '<MonitorLogItem>'


class MonitorLogRaw:
    def __init__(self, monitor_log_raw_data):
        self.monitor_log_raw_data = monitor_log_raw_data

    def __repr__(self):
        if self.monitor_log_raw_data is None:
            return 'None'
        else:
            return '<MonitorLogRawItem>'


class ErrorLogRaw:
    def __init__(self, error_log_raw_data):
        self.error_log_raw_data = error_log_raw_data

    def __repr__(self):
        if self.error_log_raw_data is None:
            return 'None'
        else:
            return '<ErrorLogRawItem>'


class TraceLogRaw:
    def __init__(self, trace_log_raw_data):
        self.trace_log_raw_data = trace_log_raw_data

    def __repr__(self):
        if self.trace_log_raw_data is None:
            return 'None'
        else:
            return '<TraceLogRawItem>'


class TraceMessage:
    def __init__(self, trace_message):
        self.trace_message = trace_message

    def __repr__(self):
        if self.trace_message is None:
            return 'None'
        else:
            return '<TraceMessageItem>'


class ErrorMessage:
    def __init__(self, error_message):
        self.error_message = error_message

    def __repr__(self):
        if self.error_message is None:
            return 'None'
        else:
            return '<ErrorMessageItem>'