import json

def filter_education_content(input_file='minister_with_neo4j_content.json', output_file='minister_education_filtered.json'):
    """
    Filtert die JSON-Datei und entfernt die häufigsten Felder, die keine Bildungsinformationen enthalten
    """
    # JSON laden
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Felder die gefiltert werden sollen (häufigste, keine Bildungsinfos)
    fields_to_filter = {
        '#',  # Root-Element - 165x
        'Weblinks',  # 163x
        'Einzelnachweise',  # 158x
        'Literatur',  # 114x
        'Abgeordneter',  # 46x
        'Abgeordnete',  # 13x
        'Siehe auch'  # 41x
    }
    
    # Gefilterte Daten erstellen
    filtered_data = []
    total_sections_before = 0
    total_sections_after = 0
    
    for person in data:
        person_copy = person.copy()
        
        # neo4j_content filtern
        if 'neo4j_content' in person_copy:
            original_content = person_copy['neo4j_content']
            total_sections_before += len(original_content)
            
            # Nur Sections behalten, die nicht in der Filter-Liste stehen
            filtered_content = []
            for content in original_content:
                if content.get('section_header') not in fields_to_filter:
                    filtered_content.append(content)
            
            person_copy['neo4j_content'] = filtered_content
            total_sections_after += len(filtered_content)
        
        filtered_data.append(person_copy)
    
    # Gefilterte JSON speichern
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, ensure_ascii=False, indent=2)
    
    # Statistiken ausgeben
    print(f"=== FILTERUNG ERFOLGREICH ===")
    print(f"Gefilterte JSON gespeichert als: {output_file}")
    print(f"Anzahl Personen: {len(data)}")
    print(f"Gefilterte Felder: {', '.join(fields_to_filter)}")
    print(f"Section-Headers vor Filterung: {total_sections_before}")
    print(f"Section-Headers nach Filterung: {total_sections_after}")
    print(f"Entfernte Sections: {total_sections_before - total_sections_after}")
    print(f"Reduktion: {((total_sections_before - total_sections_after) / total_sections_before * 100):.1f}%")
    
    # Verbleibende Section-Headers analysieren
    remaining_headers = {}
    for person in filtered_data:
        if 'neo4j_content' in person:
            for content in person['neo4j_content']:
                header = content.get('section_header', '')
                remaining_headers[header] = remaining_headers.get(header, 0) + 1
    
    print(f"\n=== VERBLEIBENDE SECTION-HEADERS (Top 20) ===")
    sorted_headers = sorted(remaining_headers.items(), key=lambda x: x[1], reverse=True)
    for header, count in sorted_headers[:20]:
        percentage = (count / len(data)) * 100
        print(f"  {header:<40} | {count:>3}x | {percentage:>5.1f}% der Personen")

if __name__ == "__main__":
    filter_education_content() 