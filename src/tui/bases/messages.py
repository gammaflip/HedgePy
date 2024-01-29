from textual.messages import Message


class LoginMessage(Message):
    def __init__(self, dbname: str, dbport: str, dbuser: str, dbpass: str):
        super().__init__()
        self.dbname = dbname
        self.dbport = dbport
        self.dbuser = dbuser
        self.dbpass = dbpass

