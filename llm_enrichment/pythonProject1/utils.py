import tiktoken
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import functools
import time

def count_tokens(text, model="gpt-4"):
    """
    Zählt die Tokens in einem Text mit tiktoken.
    """
    try:
        # Encoder für das gewählte Modell laden
        encoding = tiktoken.encoding_for_model(model)
        
        # Tokens zählen
        tokens = encoding.encode(text)
        token_count = len(tokens)
        
        return token_count
    
    except Exception as e:
        print(f"Fehler beim Token-Zählen: {e}")
        return None


def timer_decorator(func):
    """Decorator zum Messen der Ausführungszeit"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print(f"🚀 Starte {func.__name__}...")

        result = func(*args, **kwargs)

        end_time = time.time()
        execution_time_seconds = end_time - start_time
        minutes = int(execution_time_seconds // 60)
        seconds = int(execution_time_seconds % 60)

        if minutes > 0:
            print(f"✅ {func.__name__} abgeschlossen in {minutes} min {seconds} sek")
        else:
            print(f"✅ {func.__name__} abgeschlossen in {seconds} sek")

        return result

    return wrapper

