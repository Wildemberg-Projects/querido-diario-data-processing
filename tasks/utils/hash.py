import hashlib, os
from typing import Union


def hash_content(content: Union[str, bytes]) -> str:
    """
    Receives a content of string or byte format and returns its SHA-256 hash
    """

    # Verifica se o conteúdo está em bytes ou str
    conteudo_hash = content.encode('utf-8') if isinstance(content, str) else content

    # Converta o resultado para uma representação legível (hexadecimal)
    result_hash = hashlib.sha256(conteudo_hash).hexdigest()

    print(result_hash)

    return result_hash