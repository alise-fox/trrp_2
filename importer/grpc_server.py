import grpc
from concurrent import futures
from grpc_alice import books_pb2, books_pb2_grpc
from importer.db_postgres import insert_normalized
from importer.utils import read_config, create_table

class BookTransferServicer(books_pb2_grpc.BookTransferServicer):
    def __init__(self, config):
        self.config = config

    def SendBook(self, request_iterator, context):
        count = 0
        error = False
        for book in request_iterator:
            row = (
                book.id, book.title, book.author, book.genre, book.publisher,
                book.year, book.borrower_name, book.borrower_address, book.borrow_date
            )
            try:
                insert_normalized(self.config, row)
            except Exception as e:
                error = True
                break
            count += 1
        if not error:
            return books_pb2.BookReply(success=True, message=f"Inserted {count} rows successfully")
        else:
            return books_pb2.BookReply(success=False, message="Error")

def serve():
    config = read_config("config/config.yaml")

    with open(config["grpc"]["key_path"], "rb") as f:
        private_key = f.read()
    with open(config["grpc"]["cert_path"], "rb") as f:
        certificate_chain = f.read()
    server_credentials = grpc.ssl_server_credentials(
        [(private_key, certificate_chain)]
    )

    create_table(config)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    books_pb2_grpc.add_BookTransferServicer_to_server(BookTransferServicer(config), server)
    server.add_secure_port('[::]:50051', server_credentials) 
    print("gRPC сервер запущен на порту 50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
