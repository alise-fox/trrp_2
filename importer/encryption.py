from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
import pickle

def decrypt_message(config, packet):
    d = pickle.loads(packet)
    with open(config['encryption']['private_key_path'], "rb") as f:
        private_key = RSA.import_key(f.read())
    cipher_rsa = PKCS1_OAEP.new(private_key)
    session_key = cipher_rsa.decrypt(d['enc_session_key'])
    cipher_aes = AES.new(session_key, AES.MODE_EAX, nonce=d['nonce'])
    data = cipher_aes.decrypt_and_verify(d['ciphertext'], d['tag'])
    row = pickle.loads(data)
    return row