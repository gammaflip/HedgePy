from PyQt6.QtWidgets import QWidget, QTreeWidget, QTreeWidgetItem
from PyQt6.QtCore import pyqtSignal, pyqtSlot
from src.api import api

class DBTreeExplorer(QTreeWidget):
    def __init__(self):
        ...

    # add slot for database authentication
    @pyqtSlot(api.Connection)
    def add_connection(self, conn: api.Connection):
        ...



class DBTab(QWidget):
    def __init__(self):
        super().__init__()
