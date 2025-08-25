from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from utils import count_tokens
import numpy as np
import openai
from dotenv import load_dotenv
import os

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY



def test_extract_content_sections(person):
    """Extrahiert alle Content-Abschnitte von einem Politiker"""
    all_sections = []  

    for content in person.get('neo4j_content', []):
        all_sections.append({
            'politician_name': f"{person['Vorname']} {person['Nachname']}",
            'content_id': content['content_id'],
            'section_content': content['section_content']
        })

    print(f"Gesamtanzahl Abschnitte: {len(all_sections)}")
    return all_sections

def extract_content_sections(person):
    """Extrahiert alle Content-Abschnitte von einem Politiker"""
    all_sections = []  

    for content in person.get('neo4j_content', []):
        all_sections.append({
            'politician_name': person['full_name'],
            'content_id': content['content_id'],
            'section_content': content['section_content']
        })

    print(f"Gesamtanzahl Abschnitte: {len(all_sections)}")
    return all_sections

def chunk_content_sections(sections, max_tokens=1000, overlap_tokens=200):
    """Chunkt Content-Sections die zu lang sind mit Overlap"""
    chunked_sections = []
    
    for section in sections:
        content = section['section_content']
        token_count = count_tokens(content)
        
        if token_count <= max_tokens:
            # Section ist kurz genug - einfach übernehmen
            chunked_sections.append(section)
        else:
            # Section ist zu lang - aufteilen
            words = content.split()
            current_chunk = []
            current_tokens = 0
            chunk_counter = 0
            
            for word in words:
                word_tokens = count_tokens(word + " ")
                current_chunk.append(word)
                current_tokens += word_tokens
                
                if current_tokens >= max_tokens:
                    # Chunk ist voll - speichern
                    chunk_text = " ".join(current_chunk)
                    chunked_sections.append({
                        'politician_name': section['politician_name'],
                        'content_id': f"#{chunk_counter:02d}_{section['content_id']}",
                        'section_content': chunk_text
                    })
                    
                    # Overlap für nächsten Chunk vorbereiten
                    overlap_words = []
                    overlap_tokens_count = 0
                    for word in reversed(current_chunk):
                        word_tokens = count_tokens(word + " ")
                        if overlap_tokens_count + word_tokens <= overlap_tokens:
                            overlap_words.insert(0, word)
                            overlap_tokens_count += word_tokens
                        else:
                            break
                    
                    current_chunk = overlap_words
                    current_tokens = overlap_tokens_count
                    chunk_counter += 1
            
            # Letzten Chunk hinzufügen falls noch Inhalt vorhanden
            if current_chunk:
                chunk_text = " ".join(current_chunk)
                chunked_sections.append({
                    'politician_name': section['politician_name'],
                    'content_id': f"#{chunk_counter:02d}_{section['content_id']}",
                    'section_content': chunk_text
                })
    
    return chunked_sections

def embed_sections(sections, model):
    """Erstellt Embeddings für Abschnittstexte mit dem gewählten Modell"""

    if not isinstance(model, SentenceTransformer):
        raise TypeError("Der Parameter 'model' muss ein SentenceTransformer Objekt sein")
    
    texts = [section['section_content'] for section in sections]
    embeddings = model.encode(texts, show_progress_bar=True, convert_to_tensor=False)
    
    return np.array(embeddings)


def embed_sections_openai(sections, model="text-embedding-3-small", batch_size=200):
    """Erstellt Embeddings für Abschnittstexte mit OpenAI Embedding Model"""
    
    texts = [section['section_content'] for section in sections]
    embeddings = []
    total_tokens = 0  # NEU: Token-Zähler
    
    # Batch-weise verarbeiten wegen Rate Limits
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i + batch_size]
        
        response = openai.embeddings.create(
            model=model,
            input=batch_texts
        )
        
        batch_embeddings = [data.embedding for data in response.data]
        embeddings.extend(batch_embeddings)
        
        # NEU: Token-Anzahl aus Response extrahieren
        total_tokens += response.usage.total_tokens
    
    # NEU: Token-Anzahl und Embeddings zurückgeben
    return np.array(embeddings), total_tokens

EDUCATION_QUERY = [
    "Hat die Person studiert? Welche Schule hat sie besucht? Was ist über ihren Bildungsweg bekannt? Welchen Beruf hat die Person vorher ausgeübt? Hat die Person eine Ausbildung gemacht?"
]


def find_top_k_sections(query_embedding, section_embeddings, all_sections, top_k=10):
    """Findet die ähnlichsten Content-Abschnitte für eine Query"""
    similarities = cosine_similarity([query_embedding], section_embeddings)[0]
    
    # Top-k Indizes finden
    top_indices = np.argsort(similarities)[::-1][:top_k]
    
    results = []
    for idx in top_indices:
        results.append({
            'section': all_sections[idx],
            'similarity': similarities[idx],
            'content_preview': all_sections[idx]['section_content'][:300] + "..."
        })
    
    return results
