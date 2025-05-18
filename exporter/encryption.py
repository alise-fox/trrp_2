from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.Random import get_random_bytes
import pickle

def encrypt_message(config, data):
    # 1. Получить открытый ключ
    with open(config['encryption']['public_key_path'], "rb") as f:
        public_key = RSA.import_key(f.read())
    cipher_rsa = PKCS1_OAEP.new(public_key)
    # 2. Сгенерировать AES ключ
    session_key = get_random_bytes(32)
    enc_session_key = cipher_rsa.encrypt(session_key)
    # 3. Зашифровать данные
    cipher_aes = AES.new(session_key, AES.MODE_EAX)
    data_bytes = pickle.dumps(data)
    ciphertext, tag = cipher_aes.encrypt_and_digest(data_bytes)
    # 4. Вернуть всё одним пакетом (можно pickle, можно JSON + base64, здесь для простоты pickle)
    packet = pickle.dumps({
        "enc_session_key": enc_session_key,
        "nonce": cipher_aes.nonce,
        "tag": tag,
        "ciphertext": ciphertext
    })
    return packet