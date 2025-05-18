import argparse
from utils import read_config
from sender_socket import send_via_socket
from sender_rabbitmq import send_via_rabbitmq
from utils import read_books, create_table, fill_example_data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["socket", "rabbitmq"], required=True)
    args = parser.parse_args()
    config = read_config("../config/config.yaml")

    create_table()
    fill_example_data()
    
    data_iter = read_books(config['sqlite_db'])

    if args.mode == "socket":
        send_via_socket(config, data_iter)
    elif args.mode == "rabbitmq":
        send_via_rabbitmq(config, data_iter)

if __name__ == "__main__":
    main()