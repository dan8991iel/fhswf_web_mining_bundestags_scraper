from text_to_dqr import create_sample, process_and_log, system_prompt, text_to_dqr, PRICE_DATA, calculate_cost
from embedding_retrieval import extract_content_sections, embed_sections, find_top_k_sections, chunk_content_sections, embed_sections_openai, education_query
import json
from datetime import datetime
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import os
from utils import timer_decorator, count_tokens


@timer_decorator
def main():
    #log data
    log_data = []

    test_file_name = "gpt-4.1_v6_openai_embedding"

    #load data
    data_path = os.path.join("testdata", "filtered_minister_with_content.json")
    with open(data_path, "r") as f:
        data = json.load(f)

    """
    device = "cpu"
    embedding_model = SentenceTransformer(
        "Qwen/Qwen3-Embedding-0.6B",
        trust_remote_code=True,
        device=device
    )
   
    print(f"Modell geladen: {embedding_model}")

    # HARTE KAPPUNG gegen 32k
    embedding_model.max_seq_length = 1024
    embedding_model.tokenizer.model_max_length = 1024
    """
  
    #embedding_model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')

    query_embedding = embed_sections_openai([{'section_content': education_query[0]}])[0]

    language_model = "gpt-4.1"

    for i, person in enumerate(data):
        sections = extract_content_sections(person)
        chunked_sections = chunk_content_sections(sections)
        embeddings = embed_sections_openai(chunked_sections)
        top_k_sections = find_top_k_sections(query_embedding, embeddings, chunked_sections)

        num_sections = min(5, len(top_k_sections))
        top_5 = " ".join([top_k_sections[i]['section']['section_content'] for i in range(num_sections)])

        dqr_predict, comment_predict, confidence_score, prompt_tokens, completion_tokens, cached_tokens = text_to_dqr(language_model, system_prompt, top_5)
        print(dqr_predict, comment_predict, confidence_score)
        print(f"Prompt tokens: {prompt_tokens}, Completion tokens: {completion_tokens}, Cached tokens: {cached_tokens}")
        estimated_costs = calculate_cost(language_model, prompt_tokens, completion_tokens, PRICE_DATA)

        entry = {
            "ID": person["ID"],
            "timestamp": datetime.now().isoformat(),

            "model": language_model,
            "vorname": person["Vorname"],
            "nachname": person["Nachname"],
            "dqr_original": person["DQR Niveau"],
            "comment_original": person["Höchster Abschluss nach DQR"],
            "dqr_predict": dqr_predict,
            "comment_predict": comment_predict,
            "confidence_score": confidence_score,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "estimated_costs": estimated_costs,
            "retrieved_content": top_5
        }
        log_data.append(entry)

        print(f"Processed {i+1} of {len(data)}")

    # Stelle sicher, dass der log_files Ordner existiert
    os.makedirs("log_files", exist_ok=True)
    
    with open(f"log_files/{test_file_name}.json", "w") as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)



if __name__ == '__main__':
    main()

    
