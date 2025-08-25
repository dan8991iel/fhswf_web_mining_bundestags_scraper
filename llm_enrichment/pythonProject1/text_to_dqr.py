import openai
from dotenv import load_dotenv
import os
import json
import random
from datetime import datetime

# Preise pro 1 M Tokens
PRICE_DATA_LLM = {
    "gpt-5": {
        "prompt": 1.25,
        "completion": 10.00,
    },
    "gpt-4.1": {
        "prompt":     2.00,
        "completion": 8.00,
    },
    "gpt-4.1-mini": {
        "prompt":     0.40,
        "completion": 1.60,
    },
    "gpt-4.1-nano": {
        "prompt":     0.10,
        "completion": 0.40,
    },
    "gpt-4.5-preview": {
        "prompt":     75.00,
        "completion": 150.00,
    },
    "gpt-4o": {
        "prompt":     2.50,
        "completion": 10.00,
    },
    "gpt-4o-mini": {
        "prompt":     0.15,
        "completion": 0.60,
    },
    "o1": {
        "prompt":     15.00,
        "completion": 60.00,
    },
    "o1-pro": {
        "prompt":     150.00,
        "completion": 600.00,
    },
    "o3": {
        "prompt":     10.00,
        "completion": 40.00,
    },
    "o4-mini": {
        "prompt":     1.10,
        "completion": 4.40,
    },
    "o3-mini": {
        "prompt":     1.10,
        "completion": 4.40,
    },
    "o1-mini": {
        "prompt":     1.10,
        "completion": 4.40,
    },
}

PRICE_DATA_EMBEDDING = {
    "openai": {
        "text-embedding-3-small":     0.01,
        "text-embedding-3-large":     0.065,
    }
}

def calculate_cost(model, prompt_tokens, completion_tokens, PRICE_DATA):
    # Prüfen ob es sich um ein Embedding-Modell handelt
    if model in PRICE_DATA_EMBEDDING["openai"]:
        # Embedding-Kosten (pro 1000 Token)
        total_tokens = prompt_tokens + completion_tokens
        return total_tokens * PRICE_DATA_EMBEDDING["openai"][model] / 1_000_000
    else:
        # LLM-Kosten (pro 1M Token)
        prompt_costs = prompt_tokens * PRICE_DATA[model]["prompt"] / 1_000_000
        completion_costs = completion_tokens * PRICE_DATA[model]["completion"] / 1_000_000
        return prompt_costs + completion_costs

# .env-Datei laden
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY
SYSTEM_PROMPT = """
    Du bist ein Experte für die deutsche Bildungsordnung. Du bekommst kurze biographische Texte von Politikern. Diese sind aus Wikipedia-Artikeln extrahiert.
    Deine Aufgabe ist es, aus dem Text den höchsten erreichten Bildungsabschluss nach dem Deutschen Qualifikationsrahmen (DQR) zu erkennen und zurückzugeben.
    
    AUSGABEFORMAT (streng!):
    - Entweder exakt: DQR-Niveau; Sehr kurzer Kommentar zum höchsten Bildungsabschluss; Confidence
      Beispiele:
      8; Dr. der Wirtschaftspsychologie; 5
      7; Volljurist mit 2. Staatsexamen; 4
    - ODER (nur wenn keinerlei bildungsrelevante Information in ALLEN Abschnitten vorhanden ist):
      0; Keine Angabe zum Bildungsabschluss; - 
    
    Hinweis zum Kommentar:
    - Der Kommentar bleibt sehr kurz. Für 0-Fälle gib einen knappen Grund an (z. B. „vermutlich falsches Retrieval“ oder „explizit: ohne Abschluss/Abbruch“).
    
    Confidence-Score (Definition):
    C=3 (sehr sicher): Explizite Nennung des Abschlusses/Grades/Titels (z. B. „2. Staatsexamen“, „Diplom (FH)“, „Diplom-Kaufmann (Univ.)“, „Dr. …“), oder rechtlich geschützter Status, der zwingend den Abschluss voraussetzt (z. B. zugelassener Rechtsanwalt).
    C=2 (plausibel): Gute implizite Evidenz über typische Qualifikationswege/Laufbahnen oder historische Gleichstellungen oder schwache implizite Evidenz, aber mindestens ein klares Bildungssignal vorhanden.
    C=1 (vage Annahme): ZB. bei expliziten Abbrüchen/„ohne Abschluss“ ⇒ gib 0; …; 5 oder wenn du dir sehr unsicher bist.
    C=0 Keine Angabe zum Bildungsabschluss
    
    Nutze nur die offiziellen DQR-Stufen von 1 bis 8. Die Zuordnung der jeweiligen Abschlüsse zu den Niveaus kannst du der folgenden Liste entnehmen. Wenn der gefundene Abschluss nicht darin enthalten ist, dann entscheide, welchem dieser Abschlüsse er am nächsten kommt und kategorisiere dementsprechend (konservative Annahme).
    
    Niveau 1:
    Berufsausbildungsvorbereitung
      Berufsvorbereitende Bildungsmaßnahmen der Arbeitsagentur (BvB, BvB-Reha)
      Berufsvorbereitungsjahr (BVJ)
    
    Niveau 2:
    Berufsausbildungsvorbereitung
      Berufsvorbereitende Bildungsmaßnahmen der Arbeitsagentur (BvB, BvB-Reha)
      Berufsvorbereitungsjahr (BVJ)
      Einstiegsqualifizierung (EQ)
    Berufsfachschule (Berufliche Grundbildung)
    Erster Schulabschluss (ESA)/Hauptschulabschluss (HSA)
    
    Niveau 3:
    Duale Berufsausbildung (2-jährige Ausbildungen)
    Berufsfachschule (Mittlerer Schulabschluss)
    Mittlerer Schulabschluss (MSA, „mittlere Reife“)
    
    Niveau 4:
    Duale Berufsausbildung (3- und 3 ½-jährige Ausbildungen)
    Berufsfachschule (Assistentenberufe)
    Berufsfachschule (vollqualifizierende Berufsausbildung)
    Berufsfachschule (Landesrechtlich geregelte Berufsausbildungen)
    Berufsfachschule (Bundesrechtliche Ausbildungsregelungen für Berufe im Gesundheitswesen und in der Altenpflege)
    Fachhochschulreife (FHR)
    Fachgebundene Hochschulreife (FgbHR)
    Allgemeine Hochschulreife (AHR, „Abitur“)
    Berufliche Ausbildung („Kaufmann/Kauffrau“)
    Berufliche Umschulung nach BBiG (Niveau 4)
      Fachkraft Bodenverkehrsdienst im Luftverkehr (Geprüfte)
    Berufliche Ausbildung („Kaufmann/Kauffrau“)
      Fachhochschulreife (FHR), fachgebundene Hochschulreife, allgemeine Hochschulreife (Abitur)
    
    Niveau 5:
    IT-Spezialist (Zertifizierter)*
    Servicetechniker (Geprüfter)*
    Sonstige berufliche Fortbildungsqualifikationen nach § 53 BBiG bzw. § 42 a HwO (Niveau 5)
    Berufliche Fortbildungsqualifikationen nach § 54 BBiG bzw. § 42 a HwO (Niveau 5)
    
    Niveau 6:
    Bachelor
    Fachkaufmann (Geprüfter)*
    Fachschule (Staatlich Geprüfter …)
    Fachschule (Landesrechtlich geregelte Weiterbildungen)
    Fachwirt (Geprüfter)*
    Meister (Geprüfter)*
    Operativer Professional (IT) (Geprüfter)*
    Sonstige berufliche Fortbildungsqualifikationen nach § 53 BBiG bzw. § 42 a HwO (Niveau 6)
    Berufliche Fortbildungsqualifikationen nach § 54 BBiG bzw. § 42 a HwO (Niveau 6)
    Erstes Staatsexamen/Lehrprüfung (z. B. Jura, Lehramt): DQR-Niveau 6
    Bei Diplom an einerFachhochschule, z. B. „Diplom-Ingenieur (FH)“) ohne Zusatz „Universität“: DQR-Niveau 6
    Bakkalaureus/Bachelor jeder Art: DQR-Niveau 6
    Fachwirt (Geprüfter), Meister, Techniker oder „Geprüfter Fachkaufmann/-frau“: DQR-Niveau 6
    
    Niveau 7:
    Master
    Strategischer Professional (IT) (Geprüfter)*
    Zweites Staatsexamen/Lehrprüfung
    Sonstige berufliche Fortbildungsqualifikationen nach § 53 BBiG bzw. § 42 a HwO (Niveau 7)
      Berufspädagoge (Geprüfter)
      Betriebswirt nach dem Berufsbildungsgesetz (Geprüfter)
      Betriebswirt nach der Handwerksordnung (Geprüfter)
      Technischer Betriebswirt (Geprüfter)
    Zweites Staatsexamen („Volljurist“): DQR-Niveau 7 (Wenn jemand als Rechtsanwalt zugelassen ist, muss er ein zweites Staatsexamen haben)
    Bei Diplom an einer Universität: Immer wie Master einstufen – DQR-Niveau 7. Z. B. Diplom-Ingenieur (Univ.), Diplom-Kaufmann, Diplom-Volkswirt (Universität)
    Bei Diplom wird zwischen Universität (Niveau 7) und Fachhochschule (Niveau 6) unterschieden.
    Magister Artium (M.A.), Master jeden Typs: DQR-Niveau 7
    Geprüfter Betriebswirt (nach HwO/BBiG) und gleichgestellte berufliche Fortbildungsabschlüsse auf diesem Niveau: DQR-Niveau 7
    
    Niveau 8:
    Promotion
    Doktorat und äquivalente künstlerische Abschlüsse
    (Ehrendoktorat zählt NICHT)
    
    Zusätzliche Entscheidungsregeln:
    
    1) Datenlage & Retrieval:
    - Du erhältst die Top-5 Abschnitte mit höchster Korrelation zur „Bildungs“-Query; sie sind ohne Reihenfolge konkateniert.
    - Prüfe ALLE Abschnitte auf Abschlussbegriffe und Synonyme. Bei Widersprüchen zählt der höchste belastbare Abschluss.
    - Falls in irgendeinem Abschnitt ein Doktortitel steht, klassifiziere als 8 – auch wenn der Abschnitt evtl. „falsch“ zugeordnet ist (wie beschrieben).
    - Wenn in KEINEM Abschnitt irgendein bildungsrelevanter Hinweis vorkommt (keine Schule, kein Abschluss, keine Ausbildung, keine Titel/Grade, keine berufsrechtlichen Laufbahnen), dann gib: 0; Keine Angabe zum Bildungsabschluss (vermutlich falsches Retrieval); -
    - Wenn mindestens ein Teilabschnitt erkennbar korrekt ist (z. B. beginnt mit Familie → Schule), entscheide auf Basis der vorhandenen Informationen; sind Angaben unvollständig, klassifiziere konservativ.
    
    2) Zeit-/Begriffslogik (Bologna & Historik; Beispiele):
    - „Diplom (FH)“ ⇒ 6.
    - „Diplom“ ohne Zusatz:
      • Wenn „Universität/Technische Hochschule (TU/TH)“ genannt ist ⇒ 7.
      • Wenn nichts genannt ist: konservativ nach Kontext. Tendenz: Uni-Diplom ⇒ 7, FH-Diplom ⇒ 6. Ohne Hinweise: 6 (als konservative Annahme, C=2).
    - „Magister Artium (M.A.)“ ⇒ 7.
    - „Technische Hochschule“ (historisch universitäres Niveau) ⇒ 7.
    - „Ingenieurschule“ (historisch, grad. Ing.) ⇒ 6 (Annahme).
    - Lehramt:
      • „Assessor des Lehramts“, „2. Staatsprüfung“, „Referendariat abgeschlossen“, „Studienrat/Oberstudienrat/Schulbeamter (verbeamtet)“ ⇒ 7 (implizit zulässig).
      • Nur „Lehramtsstudium“ ⇒ 6 (falls kein Abbruch).
    - Staatsexamen Medizin/Pharmazie/Zahnmedizin ⇒ 7.
    - Jura:
      • „Erstes Staatsexamen“ ⇒ 6.
      • „Volljurist/zugelassener Rechtsanwalt/Richter/Staatsanwalt/Assessor jur.“ ⇒ 7.
    - Berufsbildung:
      • „Gesellenprüfung/Abschluss duale Ausbildung (3–3,5 J.)“ ⇒ 4.
      • „Meister/Techniker/Fachwirt/Gepr. Fachkaufmann/-frau“ ⇒ 6.
      • „Gepr. Betriebswirt (HwO/BBiG)“ ⇒ 7.
      • „VWA-Abschlüsse“ ⇒ 6 (nächstliegend zu Fachwirt/Techniker).
    - Öffentlicher Dienst (Laufbahnen):
      • „mittlerer Dienst“ ⇒ mind. 4 (Annahme).
      • „gehobener Dienst“ ⇒ 6 (Annahme).
      • „höherer Dienst“ ⇒ 7 (Annahme).
    - Schulabschlüsse:
      • AHR/FHR/FgbHR ⇒ 4; MSA ⇒ 3; ESA/HSA ⇒ 2.
      • „Schulpflicht erfüllt“/„Gymnasium ohne Abschluss verlassen“ ⇒ kein Schulabschluss.
    
    - Abbrüche:
      • Sofern nicht ausdrücklich „abgebrochen/ohne Abschluss“, gehe von Abschluss aus.
      • Wenn „ohne Abschluss/abgebrochen“ explizit genannt ⇒ stufe entsprechend niedriger ein oder 0 (wenn gar kein Abschluss vorliegt). Für 0-Fälle mit explizitem Abbruch: Confidence = 5.
    
    3) Wenn kein Bildungsabschluss erkennbar ist:
    - Nutze: 0; Keine Angabe zum Bildungsabschluss (vermutlich falsches Retrieval); -
    
    4) Prüfe deine Antwort vor Ausgabe auf:
    - Genau zwei Semikolons: „Zahl; Kommentar; Confidence“.
    - DQR-Zahl ∈ {0,…,8}. Keine weiteren Zeichen/Zeilen.
    - Kommentar kurz. Confidence ∈ {1,2,3} oder „-“ (nur beim 0-Fall ohne Signale).
    """




def create_sample(path, sample_size):
    lst = json.load(open(path, encoding="utf-8"))
    return random.sample(lst, sample_size)


def text_to_dqr(model, system_prompt, bio_text, temperature=0.1):

    if model.startswith("o"):
        temperature = 1.0

    response = openai.chat.completions.create(
        model=model,
        temperature=temperature,
        #reasoning_effort="minimal",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": bio_text}
        ]
    )

    usage = response.usage
    prompt_tokens = usage.prompt_tokens
    completion_tokens = usage.completion_tokens
    cached_tokens = usage.prompt_tokens_details.cached_tokens

    result = result = response.choices[0].message.content.strip()

    if ";" not in result:
        raise ValueError(f"Unerwartetes Format: '{result}'")
    try:
        dqr_level_str, comment, conf_str = [p.strip() for p in result.split(";", 2)]
        confidence = None if conf_str == "-" else int(conf_str)
        return int(dqr_level_str), comment, confidence, prompt_tokens, completion_tokens, cached_tokens
    except Exception as e:
        raise ValueError(f"Fehler beim Parsen der Modellantwort: '{result}'") from e


def process_and_log(model, temperature, file_name, data):

    temp_data = []

    for minister in data:
        id = minister["ID"]
        vorname = minister["Vorname"]
        nachname = minister["Nachname"]
        dqr_original = minister["DQR Niveau"]
        comment_original = minister["Höchster Abschluss nach DQR"]
        bio_text = minister["bio_section"]
        bio_text = "\n\n".join(bio_text)
        dqr_predict, comment_predict, prompt_tokens, completion_tokens = text_to_dqr(model, temperature, system_prompt, bio_text)
        estimated_costs = calculate_cost(model, prompt_tokens, completion_tokens, PRICE_DATA)

        entry = {
            "ID": id,
            "timestamp": datetime.now().isoformat(),
            "model": model,
            "temperature": temperature,
            "vorname": vorname,
            "nachname": nachname,
            "dqr_original": dqr_original,
            "comment_original": comment_original,
            "dqr_predict": dqr_predict,
            "comment_predict": comment_predict,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "estimated_costs": estimated_costs
        }
        temp_data.append(entry)

        # Pfad zur Log-Datei (Relativ zur aktuellen Datei)
    folder = os.path.join(os.path.dirname(__file__), "log_files")
    filepath = os.path.join(folder, file_name)

    # Stelle sicher, dass der Ordner existiert
    os.makedirs(folder, exist_ok=True)

    # Lade vorhandene Logs oder initialisiere leere Liste
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = []

    # Hänge die neuen Einträge an und speichere
    existing.extend(temp_data)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)



