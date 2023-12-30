import os
from PyQt6.QtCore import pyqtSignal, pyqtSlot, QSize
from PyQt6.QtWidgets import QTabWidget, QWidget, QMainWindow
from PyQt6.QtGui import QIcon
from api import session
from config import PATH
from pandas import Timestamp


class TabWidget(QTabWidget):
    def __init__(self):
        super().__init__()
#        tab_bar = self.tabBar()
        self.setTabsClosable(False)
        self.setTabPosition(QTabWidget.TabPosition.West)

    def add_tab(self, icon_path: str = 'db', name: str = 'Generic') -> int:
        icon = QIcon(os.path.join(PATH['gui_assets'], 'icons', f'{icon_path}.svg'))
        ix = self.addTab(QWidget(), icon, name)
        return ix


class ParentWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.tabs = TabWidget()
        self.tabs.add_tab(icon_path='db', name='Database')
        self.setCentralWidget(self.tabs)
        self.setWindowTitle('HedgePy')

        self.db_session: session.Session = None

    @pyqtSlot(session.Session)
    def auth_receive(self, db_session: session.Session):
        self.db_session = db_session
