request_template = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                        xmlns:ser="http://www.businessobjects.com/DataServices/ServerX.xsd">
                        <soapenv:Header>
                            <ser:session>
                                <SessionID>{session_id}</SessionID>
                            </ser:session>
                        </soapenv:Header>
                        <soapenv:Body>{request_body}</soapenv:Body>
                    </soapenv:Envelope>'''

headers = {'Content-Type': 'application/x-www-form-urlencoded',
            'SOAPAction': '<undefined>'}

