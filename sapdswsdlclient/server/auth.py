import requests
from requests import exceptions
import xml.etree.ElementTree as ET
from sapdswsdlclient.models.batch_job import BatchJob
from sapdswsdlclient.models.job_server import JobServer
from sapdswsdlclient.models.dataflow import Dataflow
from sapdswsdlclient.models.logs import Log
from sapdswsdlclient.models.repo import Repo
from sapdswsdlclient.models.realtime_service import RealtimeService
from sapdswsdlclient.templates.templates import request_template, headers
from sapdswsdlclient.exceptions.exceptions import NotSignedInError


class Server:
    def __init__(self, wsdl_url, username, password, cms_system, cms_authentication):
        """
        :param wsdl_url: WSDL file URL
        :param username: username
        :param password: password
        :param cms_system: job server's hostname
        :param cms_authentication: the options are 'secEnterprise', 'secLDAP', 'secWinAD', 'secSAPR3'
        """
        self.username = username
        self.cms_system = cms_system
        self.cms_authentication = cms_authentication
        self.wsdl_url = wsdl_url
        self.password = password

        self.request_template = request_template
        self.headers = headers

        self.session_id = ''
        self.status = None
        self.is_session_id_valid = None

        self.batch_job = BatchJob(self)
        self.job_server = JobServer(self)
        self.dataflow = Dataflow(self)
        self.log = Log(self)
        self.repo = Repo(self)
        self.realtime_service = RealtimeService(self)


    def ping(self):
        """
        :return: status code and SAP DS version if status code is 200; status code and reason if not
        """
        request = f'''
            <soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' xmlns:ser='http://www.businessobjects.com/DataServices/ServerX.xsd'>
                <soapenv:Header/>
                   <soapenv:Body>
                      <ser:PingRequest/>
                   </soapenv:Body>
                </soapenv:Envelope>
                '''
        self.headers['SOAPAction'] = 'function=Ping'
        response = requests.get(self.wsdl_url, data=request, headers=self.headers)
        status = response.status_code
        if status == 200:
            version = ET.fromstring(response.text).find('.//version').text
            return status, version
        else:
            return status, response.reason


    def logon(self):
        """
        :return: server instance object
        """
        request = f'''
            <soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' xmlns:ser='http://www.businessobjects.com/DataServices/ServerX.xsd'>
                <soapenv:Header/>
                   <soapenv:Body>
                      <ser:LogonRequest>
                         <username>{self.username}</username>
                         <password>{self.password}</password>
                         <cms_system>{self.cms_system}</cms_system>
                         <cms_authentication>{self.cms_authentication}</cms_authentication>
                      </ser:LogonRequest>
                   </soapenv:Body>
                </soapenv:Envelope>
                '''

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'SOAPAction': 'function=Logon'
        }

        try:
            response = requests.post(self.wsdl_url, data=request, headers=headers)

            if response.status_code != 200:
                root = ET.fromstring(response.text)
                faultstring = root.find('.//faultstring').text
                raise NotSignedInError(faultstring)
            else:
                root = ET.fromstring(response.text)
                session_id = root.find('.//SessionID')
                if not session_id is None:
                    self.session_id = session_id.text
                    return self.session_id
                else:
                    raise ValueError('Session ID not found in the response.')

        except exceptions.ConnectionError as e:
            raise e


    def validate_session_id(self):
        """
        :return: 0 if SessionID is valid, 1 if SessionID is invalid
        """
        if not self.session_id:

            raise NotSignedInError ('The user is not signed in.')
        else:
            auth_request_body = f'''<ser:ValidateSessionIDRequest/>'''
            auth_request = self.request_template.format(session_id=self.session_id, request_body=auth_request_body)

            self.headers['SOAPAction'] = 'function=Validate_SessionID'

            try:
                response = requests.post(self.wsdl_url, data=auth_request, headers=headers)

                if response.status_code != 200:
                    root = ET.fromstring(response.text)
                    faultstring = root.find('.//faultstring').text
                    raise NotSignedInError(faultstring)
                else:
                    root = ET.fromstring(response.text)
                    is_session_id_valid = root.find('.//Status')
                    if not is_session_id_valid is None:
                        self.is_session_id_valid = is_session_id_valid.text
                        return self.is_session_id_valid

            except exceptions.ConnectionError as e:
                raise e


    def logout(self):
        """
        :return: 'Logout complete' if successful
        """
        if not self.session_id:
            raise NotSignedInError ('The user is not signed in.')
        else:
            auth_request_body = f'''<ser:LogoutRequest/>'''
            auth_request = self.request_template.format(session_id=self.session_id, request_body=auth_request_body)

            self.headers['SOAPAction'] = 'function=Logout'

            try:
                response = requests.post(self.wsdl_url, data=auth_request, headers=headers)

                if response.status_code != 200:
                    root = ET.fromstring(response.text)
                    faultstring = root.find('.//faultstring').text
                    raise NotSignedInError(faultstring)
                else:
                    root = ET.fromstring(response.text)
                    status = root.find('.//status')
                    if not status is None:
                        self.status = status.text
                        return self.status

            except exceptions.ConnectionError as e:
                raise