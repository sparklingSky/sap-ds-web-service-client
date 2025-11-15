from sapdswsdlclient.server.auth import Server


def logon(wsdl_url, username, password, cms_system, cms_authentication):
    server_instance = Server(wsdl_url, username, password, cms_system, cms_authentication)
    server_instance.logon()
    return server_instance