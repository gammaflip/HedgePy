from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import QRunnable, QThreadPool
from components import launcher, container


def main():
    app = QApplication([])
    parent_window = container.ParentWindow()

    def auth_callback():
        parent_window.showMaximized()

    launch = launcher.Launcher(callback_on_close=auth_callback)
    launch.auth_send.connect(parent_window.auth_receive)
    launch.show()
    app.exec()
