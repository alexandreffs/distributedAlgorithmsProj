
import subprocess

IP = "172.30.191.253"
BASE_PATH = "/mnt/c/Users/alexa/Documents/GitHub/distributedAlgorithmsProj/babel-example"

cmd10101 = f'wsl bash -c "cd {BASE_PATH} && java -cp target/BabelExample.jar Main interface=eth0 port=10101"'
cmd10102 = f'wsl bash -c "cd {BASE_PATH} && java -cp target/BabelExample.jar Main interface=eth0 port=10102 contact={IP}:10101"'
cmd10103 = f'wsl bash -c "cd {BASE_PATH} && java -cp target/BabelExample.jar Main interface=eth0 port=10103 contact={IP}:10101"'
cmd10104 = f'wsl bash -c "cd {BASE_PATH} && java -cp target/BabelExample.jar Main interface=eth0 port=10104 contact={IP}:10102"'

subprocess.Popen([
    "wt",
    # Painel superior esquerdo (NODE 10101)
    "cmd", "/k", cmd10101,
    ";",
    # Dividir verticalmente → painel superior direito (NODE 10102)
    "split-pane", "--vertical", "--size", "0.5", "cmd", "/k", cmd10102,
    ";",
    # Focar no painel esquerdo e dividir horizontalmente → inferior esquerdo (NODE 10103)
    "move-focus", "left",
    ";",
    "split-pane", "--horizontal", "--size", "0.5", "cmd", "/k", cmd10103,
    ";",
    # Focar no painel superior direito e dividir horizontalmente → inferior direito (NODE 10104)
    "move-focus", "right",
    ";",
    "move-focus", "up",
    ";",
    "split-pane", "--horizontal", "--size", "0.5", "cmd", "/k", cmd10104,
])


# import subprocess
# import time

# IP = "172.30.191.253"
# BASE_PATH = "/mnt/c/Users/alexa/Documents/GitHub/distributedAlgorithmsProj/babel-example"

# nodes = [
#     ("NODE 10101", f'wsl bash -c "cd {BASE_PATH} && java -cp target/BabelExample.jar Main interface=eth0 port=10101"'),
#     ("NODE 10102", f'wsl bash -c "cd {BASE_PATH} && java -cp target/BabelExample.jar Main interface=eth0 port=10102 contact={IP}:10101"'),
#     ("NODE 10103", f'wsl bash -c "cd {BASE_PATH} && java -cp target/BabelExample.jar Main interface=eth0 port=10103 contact={IP}:10101"'),
#     ("NODE 10104", f'wsl bash -c "cd {BASE_PATH} && java -cp target/BabelExample.jar Main interface=eth0 port=10104 contact={IP}:10102"'),
# ]

# for title, cmd in nodes:
#     subprocess.Popen([
#         "wt",
#         "new-tab",
#         "--title", title,
#         "cmd", "/k", cmd
#     ])
#     time.sleep(1)    