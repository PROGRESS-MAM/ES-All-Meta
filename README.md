# CSV Updater

Schreibt einen vollstaendigen Export aller Clips inklusive aller Custom-Metadata-Felder aus der EditShare FLOW Metadata API in eine CSV-Datei. Jeder erfolgreiche Lauf erstellt die CSV komplett neu. Der Abruf laeuft ueber mehrere parallele Requester, um die Laufzeit auf wenige Stunden zu reduzieren.

## Dateien

### Skriptdateien

| Datei | Zweck |
| --- | --- |
| `metadata_csv.py` | Hauptskript mit Abruf-, Nacharbeits- und CSV-Logik |
| `csv_updater_launcher.py` | Liegt eine Ebene ueber `metadata_csv.py` und startet dieses als eigenen Prozess, damit relative Pfade und Imports im Hauptskript unveraendert funktionieren |
| `cred.env` | Zugangsdaten fuer die API |
| `toolbox/` | Lokale Kopie des Toolbox-Pakets |
| `requirements.txt` | pip-Abhaengigkeiten (siehe unten) |

### Laufzeitdateien im `base_path`

| Datei | Zweck |
| --- | --- |
| `all_clips_all_metadata.csv` | Aktuelle Ziel-CSV. Sie wird erst nach einem vollstaendig abgearbeiteten Abruf atomar ersetzt. |
| `metadata_csv_failed_clips.csv` | Liste der im Einzelabruf nicht abrufbaren Clips. Wird bei jedem erfolgreichen Lauf neu geschrieben. Spalten: `code;clip_id`. |
| `metadata_csv.log` | Lauf-, Fortschritts- und Fehlermeldungen. Wird beim Start geleert, wenn die konfigurierte Maximalgroesse ueberschritten ist. |
| `clips_dump.jsonl` | Temporäre JSON-Lines-Zwischendatei waehrend eines Laufs. Sie wird nach erfolgreichem CSV-Export geloescht. Bei einem Laufzeitabbruch bleibt sie als Zwischenstand liegen. |
| `metadata_csv.lock` | Verhindert parallele Laeufe. Ein zu altes Lock wird gemaess `ignore_lock_after_hours` ignoriert. |
| `csv_backups/` | Enthält Backups der CSV vor jedem neuen Lauf. |

## Requirements

```bash
pip install --extra-index-url https://artifacts.editshare.com/artifactory/api/pypi/editshare-pypi-public/simple \
    editshare-flow-api~=2026.2.1.0
```

## Konfiguration

Alle Einstellungen liegen im `CONFIG`-Block am Anfang von `metadata_csv.py`.

### Allgemein

| Variable | Bedeutung |
| --- | --- |
| `test_mode` | `True` = nur `test_mode_limit` Clips abrufen, `False` = alle Clips |
| `test_mode_limit` | Anzahl Clips im Testmodus |
| `pagnation_size` | Groesse des ersten API-Batches. Werte ueber ca. 2.000 fuehren zu HTTP-431-Fehlern. |
| `num_requesters` | Anzahl paralleler Requester-Threads fuer alle Batch-Ebenen ausser dem Einzelabruf. Jeder nutzt einen eigenen API-Login. |
| `stagger_seconds` | Wartezeit zwischen dem Start aufeinanderfolgender Requester |
| `csv_max_backups` | Anzahl der aufzubewahrenden CSV-Backups |

### Nacharbeit fehlgeschlagener oder unvollstaendiger Batches

| Variable | Bedeutung |
| --- | --- |
| `redo_split_factor` | Teilt die Batchgroesse pro Nacharbeits-Ebene. Bei `pagnation_size = 400` und Faktor `8` entsteht die Leiter `[400, 50, 1]`. |
| `redo_min_batch_size` | Kleinste Batchgroesse vor dem Einzelabruf. Bei `50` endet die Leiter bei `[400, 50, 1]`. |
| `single_clip_requesters` | Anzahl Requester ausschliesslich fuer die Einzelabruf-Ebene mit Batchgroesse `1`. Alle vorherigen Ebenen nutzen `num_requesters`. |

Ein Batch wird nur dann geschrieben, wenn die API mit Code `200` genau so viele Clips liefert wie angefragt. Bei einem anderen Return Code oder bei einer abweichenden Anzahl wird der gesamte Batch in der naechsten Ebene kleiner aufgeteilt.

### Laufzeit, Lock und Log

| Variable | Bedeutung |
| --- | --- |
| `max_runtime_hours` | Maximale Laufzeit fuer den Abruf. Die Schreibphase der finalen CSV liegt danach ausserhalb dieser Grenze. Bruchteile sind erlaubt, zum Beispiel `0.5` fuer einen Test. |
| `ignore_lock_after_hours` | Alter, ab dem ein vorhandenes Lock als liegengeblieben gilt und entfernt wird. Der Wert muss groesser als `max_runtime_hours` plus Schreibphase sein und unter dem Takt geplanter Laeufe liegen. |
| `max_log_size_MB` | Maximale Groesse der Haupt-Logdatei. Ist sie beim Start groesser, wird sie mit einer entsprechenden Notiz geleert. |
| `batch_heartbeat_every` | Anzahl verarbeiteter Batches zwischen zwei Fortschrittsmeldungen pro Ebene. |

### Pfade

| Variable | Bedeutung |
| --- | --- |
| `base_path` | Gemeinsamer Ausgabeordner fuer CSV, Log, Dump, Lock, Fehlerliste und Backups |
| `cred_path` | Pfad zur `cred.env` mit den API-Zugangsdaten |
| `main_log` | Pfad zur Haupt-Logdatei |
| `csv_dump_path` | Pfad zur temporären JSON-Lines-Dump-Datei |
| `csv_file_path` | Pfad zur Ziel-CSV. Sie wird atomar ersetzt: erst `.tmp` schreiben, dann umbenennen. |
| `failed_clips_path` | Pfad zur Fehlerliste `metadata_csv_failed_clips.csv` |
| `csv_backup_path` | Ordner fuer Backups der vorherigen CSV |
| `lock_path` | Pfad zur Lock-Datei |

## Deploy auf Worker-Node

1. **Kopieren nach** `var\flow\automation\scripts\csv_updater`:
   - `toolbox` – `__init__.py` und `toolbox.py`
   - `metadata_csv.py`
   - `cred.env`

2. **Kopieren nach** `var\flow\automation\scripts`:
   - `csv_updater_launcher.py`

3. **Ausfuehrbar machen:**

   ```bash
   chmod +x /var/flow/automation/scripts/csv_updater_launcher.py
   ```

4. **Start einrichten:**
   - ueber **Flow Automation** mit den Modulen **Time** und **Script Runner**, die `csv_updater_launcher.py` aufrufen.

## Ablauf beim Start

`csv_updater_launcher.py` startet `metadata_csv.py` im Unterordner `csv_updater` als eigenstaendigen Prozess.

`metadata_csv.py` fuehrt dann diese Schritte aus:

1. Ausgabeordner und Schreibrechte pruefen. Falls das Haupt-Log groesser als `max_log_size_MB` ist, wird es mit einer Notiz als erster Zeile geleert.
2. Lock pruefen und setzen. Ein vorhandenes, juengeres Lock beendet den neuen Lauf sofort. Nur ein Lock, das aelter als `ignore_lock_after_hours` ist, wird entfernt.
3. Verbindung zur Metadata-API herstellen.
4. Bestehende Ziel-CSV sichern, alte Backups ueber `csv_max_backups` hinaus loeschen und eine vorhandene Dump-Datei des vorherigen Laufs entfernen.
5. Anzahl und Liste aller Clip-IDs abrufen.
6. Alle Clips in der ersten Ebene mit `pagnation_size` abrufen, normalerweise in 400er-Batches. Die Batches werden ueber `num_requesters` parallele Requester verteilt.
7. Pro Batch Return Code und Anzahl gelieferter Clips pruefen:
   - Code `200` und gleiche Anzahl wie angefragt: alle Clips flatten und in die Dump-Datei schreiben.
   - Anderer Return Code, weniger oder mehr gelieferte Clips: nichts aus diesem Batch schreiben. Die gesamte ID-Gruppe wird fuer die naechste Ebene vorgemerkt.
8. Vorgemerkte Gruppen eine Ebene kleiner erneut abrufen. Bei der Standardkonfiguration erfolgt der Ablauf `400 -> 50 -> 1`.
9. Im Einzelabruf werden erfolgreich gelieferte Clips geschrieben. Nicht erfolgreich gelieferte Clips werden mit Return Code und Clip-ID in `metadata_csv_failed_clips.csv` geschrieben. Der Wert `200 aber keine Daten von API` bedeutet: Die API meldete Erfolg, lieferte fuer die angefragte ID aber keinen Clip.
10. Vor jedem neuen Batch die Abrufzeit pruefen. Nach `max_runtime_hours` wird der Abruf abgebrochen. Dann werden weder Ziel-CSV noch Fehlerliste ersetzt; der Stand des vorherigen erfolgreichen Laufs bleibt erhalten.
11. Nach einem vollstaendig abgearbeiteten Abruf alle im Dump vorkommenden Spalten ermitteln und die finale CSV atomar schreiben.
12. Dump-Datei loeschen, Fehlerliste fuer diesen Lauf schreiben und Abschluss mit Kennzahlen loggen.

Die Verarbeitung eines Clips selbst bleibt unveraendert: Wenn `flatten()` oder die weitere Aufbereitung eine Exception ausloest, bricht der gesamte Lauf ab.

