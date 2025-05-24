import grpc
from grpc_alice import books_pb2, books_pb2_grpc
from exporter.utils import read_books, read_config, create_table, fill_example_data


def book_generator(sqlite_db):
    for row in read_books(sqlite_db):
        yield books_pb2.BookData(
            id=row[0],
            title=row[1],
            author=row[2],
            genre=row[3],
            publisher=row[4],
            year=row[5],
            borrower_name=row[6],
            borrower_address=row[7],
            borrow_date=row[8]
        )


def run():
    config = read_config("config/config.yaml")

    with open(config["grpc"]["cert_path"], "rb") as f:
        trusted_certs = f.read()
    credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)

    channel = grpc.secure_channel(f"{config['grpc']['host']}:{config['grpc']['port']}", credentials)
    stub = books_pb2_grpc.BookTransferStub(channel)

    create_table(config)
    fill_example_data(config)

    # for row in read_books(config['sqlite_db']):
        # book = books_pb2.BookData(
        #     id=row[0],
        #     title=row[1],
        #     author=row[2],
        #     genre=row[3],
        #     publisher=row[4],
        #     year=row[5],
        #     borrower_name=row[6],
        #     borrower_address=row[7],
        #     borrow_date=row[8]
        # )
    reply = stub.SendBook(book_generator(config['sqlite_db']))
    print("Ответ от сервера:", reply.success, reply.message)

if __name__ == "__main__":
    run()
