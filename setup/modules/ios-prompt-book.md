# iOS Prompt-Book & Schnellzugriff einrichten

Anleitung zum Einrichten des Cellarbrain Prompt-Books auf dem iPhone für die Bedienung via iMessage / OpenClaw.

Drei Stufen, jeweils unabhängig nutzbar:

1. **Apple Notes Prompt-Book** — Referenzdokument mit allen Prompts
2. **iOS Text Replacements** — Kürzel für häufige Befehle
3. **Apple Shortcuts** — One-Tap Widgets mit Parametereingabe

---

## Stufe 1: Apple Notes Prompt-Book

Ein gepinntes Notiz-Dokument mit allen Beispiel-Prompts zum schnellen Nachschlagen und Kopieren.

### Schritt 1 — Prompt-Book öffnen

Die Datei `docs/prompt-book-de.md` enthält alle kategorisierten Prompts. Öffne sie im Browser oder in VS Code und kopiere den gesamten Inhalt.

### Schritt 2 — Neue Notiz erstellen

1. Öffne die **Notizen**-App auf dem iPhone (oder Mac mit iCloud-Sync)
2. Tippe auf **Neue Notiz** (⊕)
3. Titel: **Cellarbrain Prompts**
4. Füge den kopierten Inhalt ein

### Schritt 3 — Notiz formatieren

Apple Notes unterstützt einfache Formatierung:
- **Überschriften** — Markiere Kategorietitel und wähle „Überschrift"
- **Fett** — Markiere wichtige Prompts und tippe auf **B**
- **Trennlinien** — Tippe `---` und Enter für horizontale Linien

### Schritt 4 — Notiz anpinnen

1. In der Notizenliste: Wische die Notiz **nach rechts**
2. Tippe auf das **Pin-Symbol** (📌)
3. Die Notiz erscheint jetzt immer ganz oben

### Schritt 5 — Nutzen

1. Wechsle von iMessage zur Notizen-App (Swipe oder App Switcher)
2. Finde den gewünschten Prompt in der gepinnten Notiz
3. **Lange drücken** → Kopieren
4. Zurück zu iMessage → Einfügen → Senden

**Tipp:** Nutze die Suchfunktion in Notes (Wische in der Notiz nach unten) um schnell den richtigen Prompt zu finden.

---

## Stufe 2: iOS Text Replacements

Kürzel, die direkt in iMessage funktionieren — tippe das Kürzel und der volle Prompt-Text erscheint automatisch.

### Schritt 1 — Einstellungen öffnen

1. **Einstellungen** → **Allgemein** → **Tastatur** → **Textersetzung**

### Schritt 2 — Kürzel anlegen

Tippe auf **+** (oben rechts) und lege folgende Einträge an:

**Kürzel 1: Heute Abend**
- Text: `Was soll ich heute Abend öffnen?`
- Kürzel: `;tonight`

**Kürzel 2: Food Pairing**
- Text: `Was passt zu `
- Kürzel: `;pairing`
- *Hinweis: Leerzeichen am Ende, damit du direkt das Gericht eintippen kannst*

**Kürzel 3: Kellerübersicht**
- Text: `Kellerübersicht`
- Kürzel: `;stats`

**Kürzel 4: Dringend trinken**
- Text: `Welche Weine muss ich bald trinken?`
- Kürzel: `;urgent`

**Kürzel 5: Wein des Tages**
- Text: `Wein des Tages`
- Kürzel: `;wotd`

**Kürzel 6: Dinner planen**
- Text: `Dinner planen: `
- Kürzel: `;dinner`
- *Hinweis: Leerzeichen am Ende für die Menü-Eingabe*

**Kürzel 7: Hilfe**
- Text: `Was kannst du? Zeig mir Beispiele.`
- Kürzel: `;hilfe`

### Schritt 3 — Testen

1. Öffne iMessage und die Konversation mit OpenClaw
2. Tippe `;tonight` und drücke Leertaste
3. Der Text sollte automatisch zu „Was soll ich heute Abend öffnen?" werden
4. Senden

**Tipp:** Das Semikolon `;` als Präfix verhindert versehentliche Auslösung im normalen Schreibfluss.

---

## Stufe 3: Apple Shortcuts

Parameterisierte Shortcuts für komplexe Prompts — als Widget auf dem Home Screen.

### Vorbereitung

- Öffne die **Kurzbefehle**-App (vorinstalliert auf iOS)
- Die Kontaktnummer von OpenClaw muss in den Kontakten gespeichert sein

### Shortcut 1: 🍷 Heute Abend

**Zweck:** Weinempfehlung nach Anlass

1. Öffne Kurzbefehle → **+** (neuer Kurzbefehl)
2. Name: `🍷 Heute Abend`

**Aktionen hinzufügen:**

1. **Aus Menü auswählen**
   - Eingabe: `Anlass wählen`
   - Optionen:
     - `Casual / allein`
     - `Dinner mit Freunden`
     - `Romantisch`
     - `Feier`

2. **Text**
   - Falls „Casual / allein":
     `Empfehlung für einen gemütlichen Abend allein, bis 30 CHF`
   - Falls „Dinner mit Freunden":
     `Wein für Abendessen mit Freunden, 4 Personen`
   - Falls „Romantisch":
     `Romantischer Wein für heute Abend, bis 50 CHF`
   - Falls „Feier":
     `Wir feiern heute — etwas Besonderes bis 80 CHF`

3. **Nachricht senden**
   - An: *OpenClaw-Kontakt*
   - Nachricht: *Ergebnis von Schritt 2*

### Shortcut 2: 🥘 Food Pairing

**Zweck:** Wein zu einem Gericht finden

1. Name: `🥘 Pairing`

**Aktionen:**

1. **Nach Eingabe fragen**
   - Frage: `Welches Gericht?`
   - Typ: Text

2. **Text**
   - Inhalt: `Was passt zu [Eingabe]?`

3. **Nachricht senden**
   - An: *OpenClaw-Kontakt*
   - Nachricht: *Ergebnis von Schritt 2*

### Shortcut 3: 🎉 Dinner planen

**Zweck:** Mehrgängiges Menü planen

1. Name: `🎉 Dinner`

**Aktionen:**

1. **Nach Eingabe fragen**
   - Frage: `Gänge (mit | trennen)`
   - Typ: Text
   - Beispiel: `Lachs | Risotto | Lamm | Käse`

2. **Nach Eingabe fragen**
   - Frage: `Anzahl Personen`
   - Typ: Zahl
   - Standard: `4`

3. **Nach Eingabe fragen**
   - Frage: `Budget (CHF total)`
   - Typ: Zahl
   - Standard: `150`

4. **Text**
   - Inhalt: `Dinner planen für [Personen] Personen: [Gänge]. Budget [Budget] CHF.`

5. **Nachricht senden**
   - An: *OpenClaw-Kontakt*
   - Nachricht: *Ergebnis von Schritt 4*

### Shortcut 4: 📊 Keller-Status

**Zweck:** Schnelle Kellerübersicht ohne Eingabe

1. Name: `📊 Keller`

**Aktionen:**

1. **Text**
   - Inhalt: `Kellerübersicht`

2. **Nachricht senden**
   - An: *OpenClaw-Kontakt*
   - Nachricht: *Ergebnis von Schritt 1*

### Shortcut 5: ⏰ Dringend trinken

**Zweck:** Weine die bald getrunken werden müssen

1. Name: `⏰ Dringend`

**Aktionen:**

1. **Text**
   - Inhalt: `Welche Weine muss ich bald trinken?`

2. **Nachricht senden**
   - An: *OpenClaw-Kontakt*
   - Nachricht: *Ergebnis von Schritt 1*

### Shortcut 6: 🔍 Wein suchen

**Zweck:** Wein nach Name, Region oder Traube suchen

1. Name: `🔍 Suche`

**Aktionen:**

1. **Nach Eingabe fragen**
   - Frage: `Was suchst du?`
   - Typ: Text

2. **Text**
   - Inhalt: `Was weisst du über [Eingabe]?`

3. **Nachricht senden**
   - An: *OpenClaw-Kontakt*
   - Nachricht: *Ergebnis von Schritt 2*

### Shortcuts als Home Screen Widget einrichten

1. Gehe zum **Home Screen**
2. **Lange drücken** auf eine leere Fläche (Wackel-Modus)
3. Tippe **+** (oben links) → suche **Kurzbefehle**
4. Wähle das **2×2** oder **2×4** Widget
5. Platziere es auf dem Home Screen
6. Tippe auf das Widget → **Ordner wählen**
   - Erstelle vorher einen Ordner „Cellarbrain" in der Kurzbefehle-App und verschiebe alle 6 Shortcuts dorthin
7. Das Widget zeigt jetzt alle Cellarbrain-Shortcuts als Buttons

**Alternativ — Lock Screen Widget (iOS 16+):**

1. **Lange drücken** auf dem Sperrbildschirm → **Anpassen**
2. Tippe auf den Bereich **unter der Uhr**
3. Füge einen **Kurzbefehle**-Widget hinzu
4. Wähle den gewünschten Shortcut (z.B. „🍷 Heute Abend")

---

## Tipps & Tricks

### Schneller App-Wechsel

- **Swipe-Geste:** Am unteren Rand nach rechts wischen um schnell zwischen iMessage und Notizen zu wechseln
- **App Switcher:** Vom unteren Rand nach oben wischen und halten, dann die gewünschte App antippen

### Siri Integration

Alle Shortcuts können via Siri ausgelöst werden:
- „Hey Siri, Heute Abend" → startet den Shortcut
- „Hey Siri, Pairing" → fragt nach dem Gericht

### Text Replacement Tipps

- Kürzel mit `;` beginnen verhindert versehentliche Auslösung
- Leerzeichen am Ende ermöglicht direktes Weitertippen
- Kürzel werden über iCloud auf alle Geräte synchronisiert
- Maximal ca. 200 Zeichen pro Ersetzung

### Prompt-Tipps für bessere Ergebnisse

- **Wine IDs verwenden** wenn du einen bestimmten Wein meinst: „Dossier für Wine #45" statt vage Beschreibung
- **Budget angeben** für Empfehlungen: „bis 50 CHF"
- **Personenzahl** bei Dinner-Planung: „für 6 Personen"
- **Gänge mit | trennen** bei Menüplanung: „Lachs | Ente | Käse"
- **Natürliche Sprache** funktioniert — du musst keine exakte Syntax einhalten
