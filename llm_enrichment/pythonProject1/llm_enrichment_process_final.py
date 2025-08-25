from text_to_dqr import text_to_dqr, calculate_cost, SYSTEM_PROMPT, PRICE_DATA_LLM, PRICE_DATA_EMBEDDING
from embedding_retrieval import extract_content_sections, chunk_content_sections, embed_sections_openai, find_top_k_sections, EDUCATION_QUERY
from utils import timer_decorator
from datetime import datetime
import os
import json







"""
Todos:
- [ ] Testdaten in Batches zerlegen (je 500 Datensätze)
- [ ] Logging des Prozesses - 1 Logfile pro Batch
- [ ] Fehlerbehandlung, bei Fehlschlag, den Datensatz in Log-File speichern für spätere Nachbehandlung
- [ ] 
- [ ] 
- [ ] 
- [ ] 
- [ ] 
- [ ] 
"""

def embedding_process(person, query_embedding):
    sections = extract_content_sections(person)
    chunked_sections = chunk_content_sections(sections)
    embeddings, embedding_tokens = embed_sections_openai(chunked_sections)  # NEU: embedding_tokens
    top_k_sections = find_top_k_sections(query_embedding, embeddings, chunked_sections)

    num_sections = min(5, len(top_k_sections))
    top_5 = " ".join([top_k_sections[i]['section']['section_content'] for i in range(num_sections)])

    return top_5, embedding_tokens  


def llm_process(language_model, SYSTEM_PROMPT, top_5):
    return text_to_dqr(language_model, SYSTEM_PROMPT, top_5)


def create_or_load_batches(data, batch_size=100):
    """
    Erstellt Batches oder lädt sie aus dem Cache
    """
    batch_structure_file = "final_data/batches/batch_structure.json"
    
    # Prüfen ob Batches bereits existieren
    if os.path.exists(batch_structure_file):
        print("📂 Lade gespeicherte Batch-Struktur...")
        with open(batch_structure_file, 'r') as f:
            batches = json.load(f)
        print(f"✅ {len(batches)} Batches aus Cache geladen")
        return batches
    
    # Neue Batches erstellen
    print("🆕 Erstelle neue Batch-Struktur...")
    batches = []
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        batches.append(batch)
    
    # Batches speichern
    os.makedirs("final_data/batches", exist_ok=True)
    with open(batch_structure_file, 'w') as f:
        json.dump(batches, f, ensure_ascii=False, indent=2)
    
    print(f"�� {len(batches)} Batches gespeichert")
    return batches

@timer_decorator
def process_batch(batch_data, batch_num, query_embedding, language_model):
    """
    Verarbeitet einen Batch von Politikern
    
    Args:
        batch_data: Liste der Politiker in diesem Batch
        batch_num: Nummer des Batches
        query_embedding: Embedding der Bildungs-Query
        language_model: Zu verwendendes LLM-Modell
    
    Returns:
        dict: Ergebnisse des Batches mit Metadaten
    """
    batch_results = []
    batch_errors = []
    
    # Batch-spezifische Metriken
    batch_start_time = datetime.now()
    batch_embedding_costs = 0
    batch_llm_costs = 0
    batch_embedding_time = 0
    batch_llm_time = 0

    
    print(f"🚀 Starte Batch {batch_num} mit {len(batch_data)} Politikern")
    
    for i, person in enumerate(batch_data):
        try:
            # Embedding-Prozess
            embedding_start = datetime.now()
            top_5, embedding_tokens = embedding_process(person, query_embedding)
            embedding_duration = (datetime.now() - embedding_start).total_seconds()
            batch_embedding_time += embedding_duration
            
            embedding_costs = calculate_cost("text-embedding-3-small", embedding_tokens, 0, PRICE_DATA_EMBEDDING)
            batch_embedding_costs += embedding_costs

            # LLM-Prozess
            llm_start = datetime.now()
            dqr_predict, comment_predict, confidence_score, prompt_tokens, completion_tokens, cached_tokens = llm_process(language_model, SYSTEM_PROMPT, top_5)
            llm_duration = (datetime.now() - llm_start).total_seconds()
            batch_llm_time += llm_duration
            
            # Kosten berechnen
            estimated_costs = calculate_cost(language_model, prompt_tokens, completion_tokens, PRICE_DATA_LLM)
            batch_llm_costs += estimated_costs
            
            # Erfolgreichen Datensatz speichern
            entry = {
                "neo4j_element_id": person["neo4j_element_id"],
                "timestamp": datetime.now().isoformat(),
                "vorname": person["firstname"],
                "nachname": person["lastname"],
                "full_name": person["full_name"],
                "birth_year": person["birth_year"],
                "dqr_predict": dqr_predict,
                "comment_predict": comment_predict,
                "confidence_score": confidence_score,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "estimated_costs": estimated_costs,
                "retrieved_content": top_5,
                "embedding_duration": embedding_duration,
                "llm_duration": llm_duration
            }
            batch_results.append(entry)
            
            print(f"✅ Batch {batch_num}: Politiker {i+1}/{len(batch_data)} verarbeitet")
            
        except Exception as e:
            # Fehler protokollieren
            error_entry = {
                "neo4j_element_id": person.get("neo4j_element_id", "unknown"),
                "full_name": person.get("full_name", "unknown"),
                "error_type": type(e).__name__,
                "error_message": str(e),
                "timestamp": datetime.now().isoformat(),
                "batch_num": batch_num,
                "person_index": i
            }
            batch_errors.append(error_entry)
            print(f"❌ Batch {batch_num}: Fehler bei Politiker {i+1}: {e}")
    
    # Batch-Metadaten
    batch_duration = (datetime.now() - batch_start_time).total_seconds()
    
    batch_summary = {
        "batch_num": batch_num,
        "batch_size": len(batch_data),
        "successful_processing": len(batch_results),
        "failed_processing": len(batch_errors),
        "batch_start_time": batch_start_time.isoformat(),
        "batch_duration": batch_duration,
        "total_embedding_time": batch_embedding_time,
        "total_llm_time": batch_llm_time,
        "total_llm_costs": batch_llm_costs,
        "total_embedding_costs": batch_embedding_costs,
        "results": batch_results,
        "errors": batch_errors
    }
    
    print(f"🎯 Batch {batch_num} abgeschlossen: {len(batch_results)} erfolgreich, {len(batch_errors)} Fehler")
    
    return batch_summary

def save_batch_results(batch_num, batch_summary):
    """
    Speichert Batch-Ergebnisse in separate JSON-Dateien
    
    Args:
        batch_num: Nummer des Batches
        batch_summary: Ergebnisse des Batches
    """
    # Ordner erstellen falls nicht vorhanden
    os.makedirs("final_data/batches", exist_ok=True)
    os.makedirs("final_data/errors", exist_ok=True)
    
    # Batch-Ergebnisse speichern
    batch_file = f"final_data/batches/batch_{batch_num:03d}.json"
    with open(batch_file, 'w', encoding='utf-8') as f:
        json.dump(batch_summary, f, ensure_ascii=False, indent=2)
    
    # Fehler-Log speichern (falls Fehler aufgetreten sind)
    if batch_summary["failed_processing"] > 0:
        error_file = f"final_data/errors/errors_batch_{batch_num:03d}.json"
        with open(error_file, 'w', encoding='utf-8') as f:
            json.dump(batch_summary["errors"], f, ensure_ascii=False, indent=2)
    
    # Checkpoint aktualisieren
    checkpoint_file = "final_data/checkpoint.json"
    checkpoint_data = {
        "last_completed_batch": batch_num,
        "timestamp": datetime.now().isoformat(),
        "total_batches_processed": batch_num + 1
    }
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Batch {batch_num} gespeichert: {batch_file}")
    if batch_summary["failed_processing"] > 0:
        print(f"⚠️ Fehler-Log gespeichert: {error_file}")
    print(f"📝 Checkpoint aktualisiert: {checkpoint_file}")



@timer_decorator
def main():
    # Ordner erstellen
    os.makedirs("final_data", exist_ok=True)
    
    # Daten laden
    data_path = os.path.join("final_data", "neo4j_data_politicians_filtered.json")
    with open(data_path, "r") as f:
        data = json.load(f)

    # Batches erstellen/laden
    batches = create_or_load_batches(data, batch_size=100)
    
    # Query-Embedding erstellen (einmal für alle Batches)
    query_embedding, _ = embed_sections_openai([{'section_content': EDUCATION_QUERY[0]}])
    query_embedding = query_embedding[0]  # Erste Zeile als 1D-Array
    language_model = "gpt-4.1"
    
    print(f"🚀 Starte Verarbeitung von {len(batches)} Batches")
    print(f"📊 Test: Batches 6-25")
    
    # TEST: Nächste 5 Batches verarbeiten
    for batch_num in range(35, 46):  # Batches 1, 2, 3, 4, 5
        print(f"\n�� Verarbeite Batch {batch_num}...")
        batch_result = process_batch(batches[batch_num], batch_num, query_embedding, language_model)
        
        # Batch-Ergebnisse speichern
        save_batch_results(batch_num, batch_result)
        
        print(f"✅ Batch {batch_num} abgeschlossen!")
    
    print(f"\n�� Test der nächsten 5 Batches abgeschlossen!")
    print(f"📁 Ergebnisse in final_data/batches/ gespeichert")

if __name__ == '__main__':
    main()




