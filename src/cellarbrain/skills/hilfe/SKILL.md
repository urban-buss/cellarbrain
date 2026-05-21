---
name: hilfe
description: "Zeigt verfügbare Befehle und Beispiel-Prompts. Antwortet auf: Hilfe, Was kannst du?, Befehle, Beispiele."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Hilfe & Befehlsübersicht

Zeige eine kompakte Übersicht aller Fähigkeiten mit Beispiel-Prompts.

## Owner Context

Switzerland, CHF. Antwort immer auf Deutsch. Der Benutzer interagiert via iMessage.

## Trigger-Erkennung

Reagiere auf:
- "Hilfe", "Help", "?"
- "Was kannst du?", "Was geht?"
- "Befehle", "Beispiele", "Übersicht"
- "Zeig mir was du kannst"

## Antwort

Antworte mit dieser kompakten Übersicht — immer als plain text, keine Markdown-Tabellen:

---

WEINEMPFEHLUNG HEUTE ABEND
- "Was soll ich heute Abend öffnen?"
- "Romantischer Wein für heute Abend, bis 50 CHF"
- "Was passt heute Abend zu Pasta Carbonara?"

FOOD PAIRING
- "Was passt zu Raclette?"
- "Wein zu Rindsfilet mit Rotweinsauce"
- "Welches Essen passt zu Wine #123?"

DINNER PARTY
- "Dinner planen: Lachs | Risotto | Lamm | Käse"
- "4-Gänge-Dinner für 6 Personen, Budget 150 CHF"

WEIN DES TAGES
- "Wein des Tages"
- "Überrasch mich"

TRINKFENSTER
- "Welche Weine muss ich bald trinken?"
- "Ist Wine #45 schon trinkbereit?"

KELLER-ÜBERBLICK
- "Kellerübersicht"
- "Wie viele Burgunder habe ich?"
- "Wöchentliche Zusammenfassung"

WEIN-INFO & SUCHE
- "Was weisst du über Sassicaia?"
- "Dossier für Wine #123"
- "Ähnliche Weine wie Wine #45"

KELLER-LÜCKEN & KONSUM
- "Wo habe ich Lücken im Keller?"
- "Wie schnell trinke ich meinen Keller leer?"

PREISE & MARKT
- "Preis-Check Sassicaia 2019"
- "Gibt es Preis-Alerts?"

RECHERCHE
- "Recherchiere Wine #45"
- "Welche Weine brauchen noch Recherche?"

SYSTEM
- "System-Status"
- "Daten neu laden"

TIPP: Frag einfach in natürlicher Sprache — ich verstehe auch komplexe Anfragen wie "Dinner am Samstag für 6: Tatar | Steinbutt | Lamm | Käse, Budget 200 CHF".

---

## Keine Tools nötig

Dieser Skill verwendet keine MCP-Tools. Die Antwort ist ein statischer Text.

## Output Format

Always pass `format="plain"` to every tool call. The user receives responses via iMessage where Markdown tables and formatting are not supported. Plain format uses numbered lists, bullet points, and simple text separators instead.
