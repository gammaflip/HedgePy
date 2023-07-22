from PyQt6.QtWidgets import QApplication
from components import launcher, container


def main():
    app = QApplication([])
    parent_window = container.ParentWindow()

    def auth_callback():
        parent_window.showMaximized()

    launch = launcher.Launcher(callback_on_close=auth_callback)
    launch.auth.connect(parent_window.db_conn)
    launch.show()
    app.exec()
