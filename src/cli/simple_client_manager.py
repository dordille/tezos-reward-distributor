import subprocess

from exception.client import ClientException
from util.client_utils import clear_terminal_chars


class SimpleClientManager:
    def __init__(self, client_path, verbose=None) -> None:
        super().__init__()
        self.verbose = verbose
        self.client_path = client_path

    def send_request(self, cmd, verbose_override=None):
        whole_cmd = self.client_path + cmd
        verbose = self.verbose

        if verbose_override is not None:
            verbose = verbose_override

        if verbose:
            print("--> Verbose : Command is |{}|".format(whole_cmd))

        # execute client
        process = subprocess.Popen(whole_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        bytes = []

        for b in process.stdout:
            bytes.append(b)

        # if no response in stdout, read stderr
        if not bytes:
            if verbose:
                print("--- Verbose : Nothing in stdout, reading stderr...")
            for b in process.stderr:
                bytes.append(b)

        process.wait()

        buffer = b''.join(bytes).decode('utf-8')

        if verbose:
            print("<-- Verbose : Answer is |{}|".format(buffer))

        return buffer

    def sign(self, bytes, key_name):
        response = self.send_request(" sign bytes 0x03{} for {}".format(bytes, key_name))

        response = clear_terminal_chars(response)

        for line in response.splitlines():
            if "Signature" in line:
                return line.strip("Signature:").strip()

        raise ClientException(
            "Signature not found in response '{}'. Signed with {}".format(response.replace('\n'), 'key_name'))
