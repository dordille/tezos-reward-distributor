from cli.simple_client_manager import SimpleClientManager


class CommandManager:
    def __init__(self, verbose=None) -> None:
        super().__init__()
        self.client_manager = SimpleClientManager("", verbose)

    def send_request(self, cmd, verbose_override=None):
        self.client_manager.send_request(cmd, verbose_override)
