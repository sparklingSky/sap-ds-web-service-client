from functools import wraps
from sapdswsdlclient.exceptions.exceptions import NotSignedInError


def re_logon(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        ping = self._server.ping()
        if ping[0] != 200:
            raise ConnectionError(ping)
        if self._server.validate_session_id() == '1':
            try:
                self._server.logon()
            except Exception:
                raise NotSignedInError("Failed to re-authenticate after session timeout. Check the credentials.")
        return func(self, *args, **kwargs)
    return wrapper