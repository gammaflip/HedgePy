import os
from PyQt6.QtCore import pyqtSignal, pyqtSlot, QSize
from PyQt6.QtWidgets import QTabWidget, QWidget, QMainWindow
from PyQt6.QtGui import QIcon
from api import session
from config import PATH


class TabWidget(QTabWidget):
    def __init__(self):
        super().__init__()
        tab_bar = self.tabBar()
        self.setTabsClosable(False)
        self.setTabPosition(QTabWidget.TabPosition.West)

    def add_tab(self, icon_path: str = 'db', name: str = 'Generic') -> int:
        icon = QIcon(os.path.join(PATH['gui_assets'], 'icons', f'{icon_path}.svg'))
        ix = self.addTab(QWidget(), icon, name)
        return ix


class ParentWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.pool: session.Pool = None
        self.tabs = TabWidget()
        self.setCentralWidget(self.tabs)

    @pyqtSlot(str, str)
    def db_conn(self, user: str, password: str):
        profile = session.Profile(user=user)
        self.pool = session.new(password=password, base_profile=profile)
