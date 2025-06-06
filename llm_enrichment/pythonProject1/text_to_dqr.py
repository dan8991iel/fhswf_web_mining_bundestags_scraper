import dotenv
import openai
from dotenv import load_dotenv
import os

from dotenv import load_dotenv
import os

# .env-Datei laden
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY
system_prompt = """
    Du bekommst kurze biographische Texte von Politiker:innen.
    Deine Aufgabe ist es, aus dem Text den höchsten erreichten Bildungsabschluss nach dem Deutschen Qualifikationsrahmen (DQR) zu erkennen und zurückzugeben. 
    Die Antwort soll im Format sein: DQR-Niveau; Sehr kurzer Kommentar zum höchsten Bildungsabschluss.
    Beispiel: 
    8; Dr. der Wirtschaftspsychologie
    oder
    7; Volljurist mit 2. Staatsexamen
    Nutze nur die offiziellen DQR-Stufen von 1 bis 8. Die Zuordnung der jeweiligen Abschlüsse zu den Niveaus kannst du der folgenden Liste entnehmen. Wenn der gefundene Abschluss nicht darin enthalten ist, dann entscheide welchem dieser Abschlüsse er am nächsten kommt und kategorisiere dementsprechend.

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
    Mittlerer Schulabschluss (MSA)

    Niveau 4:
    Duale Berufsausbildung (3- und 3 ½-jährige Ausbildungen)
    Berufsfachschule (Assistentenberufe)
    Berufsfachschule (vollqualifizierende Berufsausbildung)
    Berufsfachschule (Landesrechtlich geregelte Berufsausbildungen)
    Berufsfachschule (Bundesrechtliche Ausbildungsregelungen für Berufe im Gesundheitswesen und in der Altenpflege)
    Fachhochschulreife (FHR)
    Fachgebundene Hochschulreife (FgbHR)
    Allgemeine Hochschulreife (AHR)
    Berufliche Umschulung nach BBiG (Niveau 4)
      Fachkraft Bodenverkehrsdienst im Luftverkehr (Geprüfte)

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

    Niveau 7:
    Master
    Strategischer Professional (IT) (Geprüfter)*
    Sonstige berufliche Fortbildungsqualifikationen nach § 53 BBiG bzw. § 42 a HwO (Niveau 7)
      Berufspädagoge (Geprüfter)
      Betriebswirt nach dem Berufsbildungsgesetz (Geprüfter)
      Betriebswirt nach der Handwerksordnung (Geprüfter)
      Technischer Betriebswirt (Geprüfter)

    Niveau 8:
    Promotion
    Doktorant und äquivalente künstlerische Abschlüsse

    Wenn kein Bildungsabschluss erkennbar ist, antworte mit 
    0; Keine Angabe zum Bildungsabschluss
    Es könnte nämlich sein, dass bei dem Crawling der Daten versehentlich der falsche Abschnitt aus dem Wikipedia-Artikel herausgezogen wurde.
    Solltest du das Gefühl haben, dass das der Fall ist, kannst du das noch explizit angeben in dem Kommentarfeld.
    Achte darauf, dass deine Antwort 100% exakt den Angaben entspricht, denke Schritt für Schritt und überprüfe nochmal deine Antwort bevor du sie mir gibst.
    Es ist deswegen so wichtig, dass die Daten im exakten Format zurückgegeben werden, weil diese genau so in eine Datenbank geschrieben werden.
    """


def text_to_dqr(model, temperature, system_prompt, bio_text):
    response = openai.chat.completions.create(
        model=model,
        temperature=temperature,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": bio_text}
        ]
    )

    result = result = response.choices[0].message.content.strip()

    if ";" not in result:
        raise ValueError(f"Unerwartetes Format: '{result}'")
    try:
        dqr_level_str, comment = result.split(";", 1)
        return int(dqr_level_str.strip()), comment.strip()
    except Exception as e:
        raise ValueError(f"Fehler beim Parsen der Modellantwort: '{result}'") from e
