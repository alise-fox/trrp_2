import grpc
from concurrent import futures
from grpc_alice import books_pb2, books_pb2_grpc
from importer.db_postgres import insert_normalized
from importer.utils import read_config

class BookTransferServicer(books_pb2_grpc.BookTransferServicer):
    def __init__(self, config):
        self.config = config

    def SendBook(self, request, context):
        row = (
            request.id, request.title, request.author, request.genre, request.publisher,
            request.year, request.borrower_name, request.borrower_address, request.borrow_date
        )
        try:
            insert_normalized(self.config, row)
            return books_pb2.BookReply(success=True, message="Inserted successfully")
        except Exception as e:
            return books_pb2.BookReply(success=False, message=str(e))

def serve():
    config = read_config("config/config.yaml")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    books_pb2_grpc.add_BookTransferServicer_to_server(BookTransferServicer(config), server)
    server.add_insecure_port('[::]:50051')  # слушаем все интерфейсы на порту 50051
    print("gRPC сервер запущен на порту 50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
