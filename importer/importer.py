import argparse
from utils import read_config
from receiver_socket import receive_via_socket
from receiver_rabbitmq import receive_via_rabbitmq

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["socket", "rabbitmq"], required=True)
    args = parser.parse_args()
    config = read_config("../config/config.yaml")
    if args.mode == "socket":
        receive_via_socket(config)
    elif args.mode == "rabbitmq":
        receive_via_rabbitmq(config)

if __name__ == "__main__":
    main()