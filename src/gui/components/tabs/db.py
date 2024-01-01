from PyQt6.QtWidgets import QWidget, QTreeWidget, QTreeWidgetItem
from PyQt6.QtCore import pyqtSignal, pyqtSlot

import api.bases.Database
from src.api import api

class DBTreeExplorer(QTreeWidget):
    def __init__(self):
        ...

    # add slot for database authentication
    @pyqtSlot(api.bases.Database.Connection)
    def add_connection(self, conn: api.bases.Database.Connection):
        ...



class DBTab(QWidget):
    def __init__(self):
        super().__init__()
