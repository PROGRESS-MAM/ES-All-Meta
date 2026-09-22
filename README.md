# Metadata File

Schreibt einen vollstaendigen Export aller Clips inklusive aller Custom-Metadata-Felder aus der EditShare FLOW Metadata API in eine Parquet-Datei. Jeder Lauf startet frisch und erstellt die Datei komplett neu. Der Abruf laeuft ueber mehrere parallele Requester.

Die Clips werden geschachtelt geschrieben, alle Blattspalten sind Typ Text. Custom-Felder liegen unter `custom_metadata.<Feldname>`. Punkte in Feldnamen werden zu Unterstrichen, doppelte Feldnamen bekommen ein Zaehlsuffix (`_2`, `_3`).

Referenzlauf: 1.792.175 Clips, 34 Felder oberste Ebene, 417 Blattspalten, 0,8 GB, ca. 3,5 h.

## Dateien

### Skriptdateien

| Datei | Zweck |
| --- | --- |
| `metadata_file.py` | Hauptskript mit Abruf-, Schema- und Parquet-Logik |
| `metadata_file_launcher.py` | Liegt eine Ebene ueber `metadata_file/` und ruft `update_metadata_file()` auf. Flow Automation oder Standalone. |
| `cred.env` | Zugangsdaten fuer die API |
| `toolbox/` | Lokale Kopie des Toolbox-Pakets |
| `requirements.txt` | pip-Abhaengigkeiten |

### Laufzeitdateien im `out_path`

| Datei | Zweck |
| --- | --- |
| `all_clips_all_metadata.parquet` | Ziel-Datei. Wird erst nach vollstaendigem Abruf atomar ersetzt. |
| `metadata_failed_clips.csv` | Im Einzelabruf nicht abrufbare Clips. Spalten: `code;clip_id`. Wird zu Laufbeginn geloescht und nur bei Eintraegen neu geschrieben. |
| `metadata_file.log` | Lauf-, Fortschritts- und Fehlermeldungen. Wird beim Start geleert, wenn `max_log_size_MB` ueberschritten ist. |
| `clips_dump.jsonl` | Zwischendatei des laufenden Abrufs. Wird zu Laufbeginn geloescht und nach erfolgreichem Schreiben entfernt. Bei Abbruch bleibt sie als Zwischenstand liegen. |
| `metadata_file.lock` | Verhindert parallele Laeufe. Ein Lock aelter als `ignore_lock_after_hours` wird entfernt. |
| `parquet_backups/` | Backups der vorherigen Parquet-Datei. |

## Requirements

```bash
pip install --extra-index-url https://artifacts.editshare.com/artifactory/api/pypi/editshare-pypi-public/simple \
    editshare-flow-api~=2026.2.1.0
pip install pyarrow python-dotenv
```

## Konfiguration

Alle Einstellungen liegen im `CONFIG`-Block am Anfang von `metadata_file.py`.

### Allgemein

| Variable | Standard | Bedeutung |
| --- | --- | --- |
| `test_mode` | `False` | `True` = nur `test_mode_limit` Clips abrufen |
| `test_mode_limit` | `10000` | Anzahl Clips im Testmodus |
| `pagnation_size` | `400` | Groesse des ersten API-Batches. Werte ueber ca. 2.000 fuehren zu HTTP-431-Fehlern. |
| `num_requesters` | `10` | Parallele Requester-Threads fuer alle Ebenen ausser dem Einzelabruf. Jeder nutzt einen eigenen API-Login. |
| `stagger_seconds` | `5` | Wartezeit zwischen dem Start aufeinanderfolgender Requester |
| `max_backups` | `5` | Anzahl aufzubewahrender Parquet-Backups |

### Parquet

| Variable | Standard | Bedeutung |
| --- | --- | --- |
| `row_group_size` | `50000` | Zeilen pro Blockgruppe |
| `zstd_level` | `1` | zstd-Kompressionsstufe der Datenbloecke |
| `row_group_heartbeat_every` | `5` | Blockgruppen zwischen zwei Fortschrittsmeldungen der Schreibphase |

### Nacharbeit fehlgeschlagener oder unvollstaendiger Batches

| Variable | Standard | Bedeutung |
| --- | --- | --- |
| `redo_split_factor` | `8` | Teilt die Batchgroesse pro Ebene. Mit `pagnation_size = 400` entsteht die Leiter `[400, 50, 1]`. |
| `redo_min_batch_size` | `50` | Kleinste Batchgroesse vor dem Einzelabruf |
| `single_clip_requesters` | `1` | Requester fuer die Einzelabruf-Ebene mit Batchgroesse `1` |

Ein Batch wird nur geschrieben, wenn die API mit Code `200` genau so viele Clips liefert wie angefragt. Sonst wandert die gesamte ID-Gruppe eine Ebene tiefer.

### Laufzeit, Lock und Log

| Variable | Standard | Bedeutung |
| --- | --- | --- |
| `max_runtime_hours` | `18` | Maximale Laufzeit fuer den Abruf. Schema- und Schreibphase liegen ausserhalb dieser Grenze. Bruchteile erlaubt. |
| `ignore_lock_after_hours` | `20` | Alter, ab dem ein Lock als liegengeblieben gilt. Muss groesser als `max_runtime_hours` plus Schreibphase sein und unter dem Takt geplanter Laeufe liegen. |
| `max_log_size_MB` | `100` | Maximale Groesse der Haupt-Logdatei |
| `batch_heartbeat_every` | `250` | Batches zwischen zwei Fortschrittsmeldungen pro Ebene |

### Pfade

| Variable | Bedeutung |
| --- | --- |
| `out_path` | Ausgabeordner fuer Parquet, Log, Dump, Lock, Fehlerliste und Backups |
| `cred_path` | `cred.env` mit den API-Zugangsdaten, neben dem Skript |
| `main_log` | Haupt-Logdatei |
| `clips_dump_path` | JSON-Lines-Zwischendatei |
| `parquet_file_path` | Ziel-Parquet. Wird atomar ersetzt: erst `.tmp` schreiben, dann umbenennen. |
| `failed_clips_path` | Fehlerliste `metadata_failed_clips.csv` |
| `parquet_backup_path` | Backup-Ordner |
| `lock_path` | Lock-Datei |

## Deploy auf Worker-Node

1. **Kopieren nach** `/var/flow/automation/scripts/metadata_file`:
   - `toolbox` - `__init__.py` und `toolbox_mini.py`
   - `metadata_file.py`
   - `cred.env`

2. **Kopieren nach** `/var/flow/automation/scripts`:
   - `metadata_file_launcher.py`

3. **Ausfuehrbar machen:**

   ```bash
   chmod +x /var/flow/automation/scripts/metadata_file_launcher.py
   chmod +x /var/flow/automation/scripts/metadata_file/metadata_file.py
   ```

4. **Start einrichten:**
   - ueber **Flow Automation** mit den Modulen **Time** und **Script Runner**, die `metadata_file_launcher.py` aufrufen.

Alle Dateien muessen UNIX-Zeilenenden (LF) haben.

## Manueller Start

```bash
/var/flow/automation/scripts/metadata_file_launcher.py --standalone
```

Fortschritt auf stdout, Exit-Code `0` = ok, `1` = Fehler. Ohne `--standalone` laeuft der Launcher als AutomationScript, sofern `editshare_helpers` verfuegbar ist.

## Ablauf beim Start

Der Launcher importiert `metadata_file` aus dem Unterordner und ruft `update_metadata_file()` auf.

1. Ausgabeordner und Schreibrechte pruefen, Haupt-Log bei Ueberschreitung von `max_log_size_MB` leeren.
2. Lock pruefen und setzen. Ein juengeres Lock beendet den neuen Lauf sofort.
3. Verbindung zur Metadata-API herstellen.
4. Dump und Fehlerliste des letzten Laufs loeschen, bestehende Parquet-Datei sichern, Backups ueber `max_backups` hinaus loeschen.
5. Anzahl und Liste aller Clip-IDs abrufen.
6. Custom-Field-Definitionen abrufen und `db_key` auf Feldnamen abbilden.
7. Alle Clips in der ersten Ebene mit `pagnation_size` abrufen, verteilt auf `num_requesters`.
8. Pro Batch Return Code und Anzahl pruefen:
   - Code `200` und gleiche Anzahl: Custom-Felder umbenennen, Clips geschachtelt in die Dump-Datei schreiben.
   - Sonst: nichts schreiben, gesamte ID-Gruppe fuer die naechste Ebene vormerken.
9. Vorgemerkte Gruppen eine Ebene kleiner erneut abrufen, Standard `400 -> 50 -> 1`.
10. Im Einzelabruf nicht gelieferte Clips mit Return Code und Clip-ID vormerken. `200 aber keine Daten von API` bedeutet: Die API meldete Erfolg, lieferte fuer die ID aber keinen Clip.
11. Vor jedem Batch die Abrufzeit pruefen. Nach `max_runtime_hours` wird abgebrochen.
12. Schema aus dem gesamten Dump ermitteln, Parquet in Blockgruppen von `row_group_size` Zeilen schreiben, mit zstd Stufe `zstd_level` komprimieren, dann atomar ersetzen.
13. Dump loeschen, Fehlerliste schreiben, Abschluss mit Kennzahlen loggen.

## Abbruchbedingungen

Es wird nichts geschrieben, die Parquet-Datei des letzten Laufs bleibt unveraendert, der Zwischenstand bleibt in `clips_dump.jsonl`:

- Laufzeitgrenze `max_runtime_hours` erreicht
- kein einziger Clip abrufbar
- Widerspruch in der Struktur: dasselbe Feld liefert Text und Objekt oder Objekt und Liste. Einzelwert und Liste im selben Feld werden zur Liste zusammengefuehrt.
- ein Schluessel liegt nicht im Schema
- ein Clip liefert bereits ein Feld `custom_metadata`
- ein nicht abgebildeter `db_key` liefert einen bereits belegten Feldnamen

Die Meldung nennt jeweils den vollstaendigen Feldpfad.
