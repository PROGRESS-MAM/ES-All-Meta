#!/usr/bin/env python3

# --------- IMPORTS ---------
import csv
import getpass
import json
import os
import queue
import re
import shutil
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

script_path = Path(__file__).resolve().parent

if str(script_path) not in sys.path:
    sys.path.insert(0, str(script_path))

try:
    from toolbox import tb_link_api, tb_get_duration_hours_from_tc, tb_write_log
except ImportError:
    from toolbox.toolbox import tb_link_api, tb_get_duration_hours_from_tc, tb_write_log


# --------- CONFIG ---------
app_name = "Metadata File"
app_version = "1.0"

# set op-mode
test_mode = False

# clips to catch in test mode
test_mode_limit = 10000

# batch request workers
num_requesters = 10

# start workers every
stagger_seconds = 5

# clips per level 1 batches
pagnation_size = 400

# factor for calculation of level 2 batches
redo_split_factor = 8

# min size of level 2 batches
redo_min_batch_size = 50

# max requester for level 3 batches
single_clip_requesters = 1

# getting data for api can run this long
max_runtime_hours = 18

# script can run for this long
ignore_lock_after_hours = 20

# log can get this big
max_log_size_MB = 100

# number of backups to keep
max_backups = 5

# write batch request process in log every 
batch_heartbeat_every = 250

# write process of parquet file creation every
row_group_heartbeat_every = 5

# size of row group in parquet file
row_group_size = 50000

# compression level of data blocks in parquet file
zstd_level = 1

# Paths
out_path = Path("/home/editshare/FileExchange/SMB/Metadata_File")
# out_path = Path(__file__).parent

main_log = out_path / "metadata_file.log"
clips_dump_path = out_path / "clips_dump.jsonl"
parquet_file_path = out_path / "all_clips_all_metadata.parquet"
failed_clips_path = out_path / "metadata_failed_clips.csv"
parquet_backup_path = out_path / "parquet_backups"
lock_path = out_path / "metadata_file.lock"
cred_path = script_path / "cred.env"

# --------- INIT ---------
dump_lock = threading.Lock()
log_lock = threading.Lock()
leaf_shape = "leaf"

class RuntimeLimitReached(Exception):
    """Laufzeitgrenze erreicht - es wird bewusst nichts geschrieben."""

class SchemaConflict(Exception):
    """Struktur passt nicht zum Schema - es wird bewusst nichts geschrieben."""

# --------- LOG / INFRA ---------
def notify(level: str, message: str, message_handler=None):
    """Einzige Log-Schnittstelle: schreibt ins Logfile und meldet nach aussen.

    message_handler ist im Flow-Betrieb self.add_message des AutomationScripts,
    im Standalone-Betrieb None -> dann wird auf stdout gedruckt.
    """
    with log_lock:
        try:
            tb_write_log(main_log, message)
        except Exception as exc:
            print(f"Logfile nicht schreibbar ({main_log}): {exc}", file=sys.stderr)
        if message_handler is None:
            print(message)
        else:
            message_handler(level, message)


def ensure_out_path():
    """Ausgabeordner sicherstellen und Schreibrechte pruefen."""
    try:
        out_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(f"Ausgabeordner nicht erreichbar/anlegbar: {out_path} ({exc})") from exc

    if not os.access(out_path, os.W_OK):
        try:
            user = getpass.getuser()
        except Exception:
            user = "unbekannt"
        raise RuntimeError(f"Kein Schreibrecht auf {out_path} - Skript laeuft als User '{user}'")


def cap_main_log():
    """Log beim Start leeren, wenn es zu gross geworden ist."""
    limit_bytes = int(max_log_size_MB * 1024 * 1024)
    try:
        if main_log.exists() and main_log.stat().st_size > limit_bytes:
            stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            main_log.write_text(
                f"{stamp}: Log war groesser als {max_log_size_MB} MB und wurde geleert.\n",
                encoding="utf-8",
            )
    except OSError as exc:
        print(f"Log konnte nicht gekappt werden ({main_log}): {exc}", file=sys.stderr)


def acquire_lock(message_handler=None):
    """Verhindert parallele Laeufe"""
    ignore_after_seconds = ignore_lock_after_hours * 3600

    if lock_path.exists():
        age = time.time() - lock_path.stat().st_mtime
        if age < ignore_after_seconds:
            raise RuntimeError(f"Es laeuft bereits ein Update (Lock: {lock_path}, Alter {int(age)}s)")
        notify("info", f"Veraltetes Lock entfernt: {lock_path}", message_handler)
        lock_path.unlink(missing_ok=True)

    handle = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    with os.fdopen(handle, "w", encoding="utf-8") as lock_file:
        lock_file.write(f"{os.getpid()} {datetime.now().isoformat(timespec='seconds')}\n")


def release_lock():
    lock_path.unlink(missing_ok=True)


def prep_folder_files(out_file: Path, backup_path: Path, max_backups: int,
                      dump_path: Path, failed_path: Path, message_handler=None):
    """Ausgabeordner fuer einen frischen Lauf vorbereiten.

    Dump und Fehlerliste des letzten Laufs werden entfernt, eine vorhandene
    Parquet-Datei wird ins Backup kopiert und alte Backups bis auf max_backups
    geloescht. Rueckgabe: True, wenn eine Parquet-Datei vorhanden war.
    """
    for path in (dump_path, failed_path):
        if path.exists():
            path.unlink(missing_ok=True)
            notify("info", f"Datei aus dem letzten Lauf entfernt: {path}", message_handler)

    if not out_file.exists():
        notify("info", f"Keine bestehende Parquet-Datei gefunden, wird neu erstellt: {out_file}",
               message_handler)
        return False

    backup_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_file_path = backup_path / f"{stamp}_{out_file.name}"

    try:
        shutil.copy2(out_file, backup_file_path)
    except OSError as exc:
        shutil.copyfile(out_file, backup_file_path)
        notify("info", f"Backup ohne Metadaten kopiert ({exc})", message_handler)
    notify("info", f"Backup erstellt: {backup_file_path}", message_handler)

    backups = sorted(
        (
            path for path in backup_path.iterdir()
            if path.is_file() and path.name.endswith(f"_{out_file.name}")
        ),
        key=lambda path: path.name,
    )

    for oldest in (backups[:-max_backups] if max_backups else backups):
        oldest.unlink()
        notify("info", f"Backup geloescht: {oldest}", message_handler)

    return True


# --------- FUNC ---------
def build_level_sizes(batch_size: int, factor: int, min_size: int):
    """Batchgroessen der Ebenen erzeugen.

    400 mit factor=8 und min_size=50 ergibt [400, 50, 1].
    Die letzte Ebene ist immer der Einzelabruf.
    """
    factor = max(2, int(factor))
    min_size = max(1, int(min_size))

    sizes = [max(1, int(batch_size))]
    current = sizes[0]

    while True:
        next_size = current // factor
        if next_size < min_size or next_size >= current:
            break
        sizes.append(next_size)
        current = next_size

    if sizes[-1] != 1:
        sizes.append(1)

    return sizes


def write_failed_clips(entries, path: Path):
    """Nicht abrufbare Clips als CSV schreiben.

    Die Datei wurde zu Laufbeginn entfernt und entsteht nur, wenn es Eintraege gibt.
    """
    if not entries:
        return

    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["code", "clip_id"])
        for code, clip_id in entries:
            writer.writerow([code, clip_id])


def safe_field_name(name):
    """Punkte im Feldnamen ersetzen - der Searcher liest den Punkt als Pfadtrenner."""
    return str(name).replace(".", "_")


def build_dictionary(field_definitions, message_handler=None):
    """db_key der API auf einen eindeutigen Feldnamen abbilden.

    Sortiert nach db_key, damit die Namen ueber Laeufe stabil bleiben. Ein bereits
    belegter Name bekommt ein Zaehlsuffix, damit kein Wert still verloren geht.
    """
    dictionary = {}
    taken = set()

    for definition in sorted(field_definitions, key=lambda item: item["db_key"]):
        db_key = definition["db_key"]
        raw_name = definition.get("name") or db_key
        name = safe_field_name(raw_name)

        if name != raw_name:
            notify("info", f"Feldname '{raw_name}' enthaelt Punkte und wird als '{name}' "
                           f"geschrieben", message_handler)

        unique = name
        counter = 2
        while unique in taken:
            unique = f"{name}_{counter}"
            counter += 1

        if unique != name:
            notify("info", f"WARN: Feldname '{name}' ist mehrfach vergeben - db_key {db_key} "
                           f"wird als '{unique}' geschrieben", message_handler)

        taken.add(unique)
        dictionary[db_key] = unique

    return dictionary


def rename_custom_metadata(clip: dict, dictionary: dict):
    """Custom-Metadaten umbenennen und als eigenes Feld custom_metadata ablegen.

    Dadurch heissen die Blattpfade custom_metadata.<Feldname> wie die frueheren
    Spalten. Feldnamen mit Punkt werden ueber safe_field_name entschaerft.
    """
    if "custom_metadata" in clip:
        raise SchemaConflict("Clip liefert bereits ein Feld custom_metadata - Umbenennung waere nicht eindeutig")

    asset = clip.get("asset")
    if isinstance(asset, dict):
        asset.pop("customtypes", None)
        custom = asset.pop("custom", None)
        if isinstance(custom, dict):
            renamed = {}
            for key, value in custom.items():
                field_name = dictionary.get(key) or safe_field_name(key)
                if field_name in renamed:
                    raise SchemaConflict(f"custom_metadata.{field_name}: db_key {key} liefert "
                                         f"einen bereits belegten Feldnamen")
                renamed[field_name] = value
            clip["custom_metadata"] = renamed
        elif custom is not None:
            asset["custom"] = custom

    return clip


def normalize_field_name(name):
    """Einen Feldnamen auf ein stabiles Schluessel-Format fuer Vergleiche normalisieren."""
    text = str(name).strip().lower()
    text = text.replace(".", " ").replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", text)


def find_timecode_value(data, target_names: set[str]):
    """Einen Wert in einem verschachtelten Clip-Objekt anhand der Timecode-Feldnamen finden."""
    if isinstance(data, dict):
        for key, value in data.items():
            normalized_key = normalize_field_name(key)
            if normalized_key in target_names:
                return value
            nested_value = find_timecode_value(value, target_names)
            if nested_value is not None:
                return nested_value
    elif isinstance(data, list):
        for item in data:
            nested_value = find_timecode_value(item, target_names)
            if nested_value is not None:
                return nested_value
    return None


def add_duration_h_to_clip(clip: dict) -> dict:
    """Berechnet Duration_h aus den Timecode-Feldern eines Clips, falls vorhanden."""
    if not isinstance(clip, dict):
        return clip

    start_value = find_timecode_value(clip, {"tc start", "timecode start"})
    end_value = find_timecode_value(clip, {"tc end", "timecode end"})
    if start_value is None or end_value is None:
        return clip

    duration_h = tb_get_duration_hours_from_tc(str(start_value), str(end_value))
    if duration_h is None:
        return clip

    clip["duration hour"] = duration_h
    return clip


def natural_sort_key(s):
    return [
        (1, int(part)) if part.isdigit() else (0, part.lower())
        for part in re.split(r'(\d+)', s)
    ]


# --------- SCHEMA ---------
def value_kind(value):
    if isinstance(value, dict):
        return "Objekt"
    if isinstance(value, list):
        return "Liste"
    return "Text"


def shape_kind(shape):
    if isinstance(shape, dict):
        return "Objekt"
    if isinstance(shape, list):
        return "Liste"
    return "Text"


def merge_shape(shape, value, path):
    """Struktur eines Werts in die bisher gesehene Struktur einarbeiten.

    None = noch unbekannt, dict = Objekt, list = Liste mit der inneren Struktur als
    einzigem Element, leaf_shape = Blatt. Einzelwert und Liste im selben Feld werden
    zur Liste zusammengefuehrt, alle anderen Widersprueche brechen den Lauf ab.
    """
    if value is None:
        return shape

    if isinstance(value, dict):
        if shape is None:
            shape = {}
        if not isinstance(shape, dict):
            raise SchemaConflict(f"{path}: Objekt und {shape_kind(shape)} im selben Feld")
        for key, item in value.items():
            child_path = f"{path}.{key}" if path else key
            shape[key] = merge_shape(shape.get(key), item, child_path)
        return shape

    if isinstance(value, list):
        if shape is None:
            shape = []
        if isinstance(shape, dict):
            raise SchemaConflict(f"{path}: Liste und Objekt im selben Feld")
        if not isinstance(shape, list):
            shape = [shape]
        for item in value:
            inner = merge_shape(shape[0] if shape else None, item, f"{path}[]")
            if shape:
                shape[0] = inner
            else:
                shape.append(inner)
        return shape

    if isinstance(shape, dict):
        raise SchemaConflict(f"{path}: Text und Objekt im selben Feld")
    if isinstance(shape, list):
        inner = merge_shape(shape[0] if shape else None, value, f"{path}[]")
        if shape:
            shape[0] = inner
        else:
            shape.append(inner)
        return shape

    return leaf_shape


def shape_to_type(shape):
    if isinstance(shape, list):
        return pa.list_(shape_to_type(shape[0]) if shape else pa.string())
    if isinstance(shape, dict):
        if not shape:
            return pa.string()
        return pa.struct([pa.field(key, shape_to_type(shape[key]))
                          for key in sorted(shape, key=natural_sort_key)])
    return pa.string()


def build_schema(shape):
    return pa.schema([pa.field(key, shape_to_type(shape[key]))
                      for key in sorted(shape, key=natural_sort_key)])


def count_leaves(arrow_type):
    if pa.types.is_struct(arrow_type):
        return sum(count_leaves(child.type) for child in arrow_type)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return count_leaves(arrow_type.value_type)
    return 1


def count_schema_leaves(schema):
    return sum(count_leaves(field.type) for field in schema)


def collect_shape_from_dump(dump_path: Path):
    shape = {}

    with dump_path.open("r", encoding="utf-8") as dump_file:
        for line in dump_file:
            merge_shape(shape, json.loads(line), "")

    return shape


def stringify(value, shape, path):
    """Werte auf Text bringen und dabei gegen das Schema pruefen.

    pyarrow verwirft unbekannte Schluessel still, darum wird hier jeder Schluessel
    explizit gegen das Schema gehalten.
    """
    if value is None:
        return None

    if isinstance(shape, list):
        if isinstance(value, dict):
            raise SchemaConflict(f"{path}: Liste erwartet, Objekt gefunden")
        inner = shape[0] if shape else leaf_shape
        items = value if isinstance(value, list) else [value]
        return [stringify(item, inner, f"{path}[]") for item in items]

    if isinstance(shape, dict) and shape:
        if not isinstance(value, dict):
            raise SchemaConflict(f"{path}: Objekt erwartet, {value_kind(value)} gefunden")
        result = {}
        for key, item in value.items():
            child_path = f"{path}.{key}" if path else key
            if key not in shape:
                raise SchemaConflict(f"{child_path}: Feld ist im Schema nicht vorhanden")
            result[key] = stringify(item, shape[key], child_path)
        return result

    if isinstance(value, (dict, list)):
        if not value:
            return None
        raise SchemaConflict(f"{path}: Text erwartet, {value_kind(value)} gefunden")

    return str(value)


# --------- FETCH ---------
def fetch_all_clips_multi(metadata_api, clip_ids: list, pagnation: int, dump_path: Path,
                          num_requesters: int, stagger_seconds: float, deadline: float,
                          message_handler=None):
    """Holt alle Clips ueber mehrere Ebenen und schreibt sie zeilenweise als JSONL.

    Die Ebenen kommen aus build_level_sizes(), z.B. [400, 50, 1]. Pro Batch gilt
    alles-oder-nichts: nur ein vollstaendig gelieferter Batch wird geschrieben,
    alles andere wandert als Gruppe eine Ebene tiefer. Dadurch kann kein Clip
    doppelt in der Ausgabe landen, ohne dass Clip-IDs verglichen werden muessen.

    Geschrieben werden die Clips unveraendert verschachtelt, nur die Custom-Felder
    werden umbenannt.

    Rueckgabe: dict mit Kennzahlen.
    """
    custom_field_definitions = metadata_api.getCustomMetadataFields()

    if metadata_api.last_return_code() != 200:
        raise RuntimeError(
            f"Fehler beim Abrufen der Custom-Field-Definitionen: return code {metadata_api.last_return_code()}"
        )

    api_to_field = build_dictionary(custom_field_definitions, message_handler)

    dump_path.write_text("", encoding="utf-8")

    level_sizes = build_level_sizes(pagnation, redo_split_factor, redo_min_batch_size)

    stats = {"rows": 0, "levels": [], "not_available": []}
    work = [list(clip_ids)] if clip_ids else []

    for level_number, size in enumerate(level_sizes, start=1):
        if not work:
            break

        batches = []
        for group in work:
            for start in range(0, len(group), size):
                batches.append(group[start:start + size])

        is_last_level = (size == 1)
        requesters = single_clip_requesters if is_last_level else num_requesters
        requesters = max(1, min(requesters, len(batches)))

        work_queue = queue.Queue()
        for batch in batches:
            work_queue.put(batch)

        next_work = []
        level_stats = {"batches": len(batches), "rows": 0, "passed_on": 0, "done": 0}
        limit_reached = {"hit": False}
        state_lock = threading.Lock()

        def worker_task(requester_id: int):
            time.sleep(stagger_seconds * (requester_id - 1))
            worker_api = tb_link_api(cred_path, "metadata")
            if worker_api is None:
                raise RuntimeError(f"Requester {requester_id}: Verbindung zur Metadata-API fehlgeschlagen")

            while True:
                if time.monotonic() > deadline:
                    with state_lock:
                        limit_reached["hit"] = True
                    return

                try:
                    id_batch = work_queue.get_nowait()
                except queue.Empty:
                    return

                clips_batch = worker_api.getClipsByIDs(id_batch)
                code = worker_api.last_return_code()
                requested = len(id_batch)
                delivered = len(clips_batch) if clips_batch else 0

                if code != 200:
                    reason = code
                elif delivered == requested:
                    reason = None
                elif delivered < requested:
                    reason = "200 aber keine Daten von API"
                else:
                    reason = "200 aber unerwartete Anzahl"
                    notify(
                        "info",
                        f"WARN: Requester {requester_id}: Batch mit {requested} IDs "
                        f"lieferte {delivered} Clips",
                        message_handler,
                    )

                if reason is None:
                    rows = [add_duration_h_to_clip(rename_custom_metadata(clip, api_to_field))
                            for clip in clips_batch]

                    with dump_lock:
                        with dump_path.open("a", encoding="utf-8") as dump_file:
                            for row in rows:
                                dump_file.write(json.dumps(row) + "\n")

                    with state_lock:
                        stats["rows"] += len(rows)
                        level_stats["rows"] += len(rows)

                elif is_last_level:
                    with state_lock:
                        stats["not_available"].append((reason, id_batch[0]))

                else:
                    with state_lock:
                        next_work.append(id_batch)
                        level_stats["passed_on"] += 1
                    notify(
                        "info",
                        f"WARN: Requester {requester_id}: Batch mit {requested} IDs "
                        f"nicht vollstaendig (code {reason}) - eine Ebene tiefer",
                        message_handler,
                    )

                with state_lock:
                    level_stats["done"] += 1
                    done = level_stats["done"]

                if done % batch_heartbeat_every == 0:
                    notify(
                        "info",
                        f"Ebene {level_number} (Groesse {size}): {done}/{level_stats['batches']} "
                        f"Batches, {stats['rows']} Clips geholt",
                        message_handler,
                    )

        with ThreadPoolExecutor(max_workers=requesters) as executor:
            futures = [
                executor.submit(worker_task, requester_id)
                for requester_id in range(1, requesters + 1)
            ]

        for future in futures:
            future.result()

        stats["levels"].append({
            "level": level_number,
            "size": size,
            "batches": level_stats["batches"],
            "rows": level_stats["rows"],
            "passed_on": level_stats["passed_on"],
        })

        if is_last_level:
            notify(
                "info",
                f"Ebene {level_number}: {level_stats['batches']} Einzelabrufe -> "
                f"{level_stats['rows']} Clips geholt, "
                f"{len(stats['not_available'])} Clips nicht abrufbar",
                message_handler,
            )
        else:
            notify(
                "info",
                f"Ebene {level_number}: {level_stats['batches']} Batches x {size} -> "
                f"{level_stats['rows']} Clips geholt, "
                f"{level_stats['passed_on']} Gruppen weitergereicht",
                message_handler,
            )

        if limit_reached["hit"]:
            raise RuntimeLimitReached(
                f"Laufzeitgrenze von {max_runtime_hours} h auf Ebene {level_number} erreicht"
            )

        work = next_work

    for group in work:
        for clip_id in group:
            stats["not_available"].append(("unbekannt", clip_id))

    return stats


# --------- WRITE ---------
def write_parquet_from_dump(dump_path: Path, shape: dict, schema, parquet_path: Path,
                            expected_rows: int, message_handler=None):
    tmp_path = parquet_path.with_name(parquet_path.name + ".tmp")
    rows_written = 0
    row_groups = 0

    try:
        with pq.ParquetWriter(tmp_path, schema, compression="zstd",
                              compression_level=zstd_level) as writer:
            with dump_path.open("r", encoding="utf-8") as dump_file:
                block = []
                for line in dump_file:
                    block.append(stringify(json.loads(line), shape, ""))
                    if len(block) >= row_group_size:
                        writer.write_table(pa.Table.from_pylist(block, schema=schema))
                        rows_written += len(block)
                        row_groups += 1
                        block = []
                        if row_groups % row_group_heartbeat_every == 0:
                            notify("info", f"Parquet: {rows_written} Zeilen in {row_groups} "
                                           f"Blockgruppen geschrieben", message_handler)
                if block:
                    writer.write_table(pa.Table.from_pylist(block, schema=schema))
                    rows_written += len(block)
                    row_groups += 1

        if rows_written != expected_rows:
            raise RuntimeError(f"Dump enthaelt {rows_written} Zeilen, abgerufen wurden "
                               f"{expected_rows} Clips")
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    tmp_path.replace(parquet_path)

    return rows_written, row_groups


# --------- MAIN ---------
def update_metadata_file(message_handler=None, extra_args=None):
    """Baut die Parquet-Datei komplett neu. Wirft bei Fehlern eine Exception.

    Rueckgabe: dict mit Kennzahlen (fuer Flow-Messages / Logging).
    """
    ensure_out_path()
    cap_main_log()
    notify("info", f"{app_name} {app_version} started.", message_handler)
    if extra_args:
        notify("info", f"Extra arguments: {extra_args}", message_handler)

    acquire_lock(message_handler)
    try:
        metadata_api = tb_link_api(cred_path, "metadata")
        if metadata_api is None:
            raise RuntimeError(f"Verbindung zur Metadata-API fehlgeschlagen (Credentials: {cred_path})")

        prep_folder_files(parquet_file_path, parquet_backup_path, max_backups,
                          clips_dump_path, failed_clips_path, message_handler)

        if test_mode:
            limit = test_mode_limit
        else:
            limit = metadata_api.numClips()
            if metadata_api.last_return_code() != 200:
                raise RuntimeError(
                    f"Fehler beim Abrufen der Clip-Anzahl: return code {metadata_api.last_return_code()}"
                )

        clip_ids = metadata_api.clips(offset=0, limit=limit)

        if metadata_api.last_return_code() != 200:
            raise RuntimeError(
                f"Fehler beim Abrufen der Clip-IDs: return code {metadata_api.last_return_code()}"
            )

        clip_ids = list(clip_ids or [])
        notify("info", f"Geplant: {len(clip_ids)} Clips komplett neu abrufen und schreiben",
               message_handler)

        deadline = time.monotonic() + max_runtime_hours * 3600

        try:
            stats = fetch_all_clips_multi(
                metadata_api, clip_ids, pagnation_size, clips_dump_path,
                num_requesters, stagger_seconds, deadline, message_handler,
            )
        except RuntimeLimitReached as exc:
            notify(
                "info",
                f"ABBRUCH: {exc} - es wurde nichts geschrieben. Die Parquet-Datei vom letzten "
                f"Lauf bleibt unveraendert. Zwischenstand liegt in {clips_dump_path}",
                message_handler,
            )
            raise

        if clip_ids and stats["rows"] == 0:
            raise RuntimeError("Kein einziger Clip konnte abgerufen werden - Parquet-Datei bleibt unveraendert")

        try:
            shape = collect_shape_from_dump(clips_dump_path)
            schema = build_schema(shape)
            leaf_columns = count_schema_leaves(schema)
            notify("info", f"Schema: {len(schema)} Felder auf oberster Ebene, "
                           f"{leaf_columns} Blattspalten", message_handler)
            rows_written, row_groups = write_parquet_from_dump(
                clips_dump_path, shape, schema, parquet_file_path, stats["rows"], message_handler)
        except SchemaConflict as exc:
            notify(
                "info",
                f"ABBRUCH: {exc} - es wurde nichts geschrieben. Die Parquet-Datei vom letzten "
                f"Lauf bleibt unveraendert. Zwischenstand liegt in {clips_dump_path}",
                message_handler,
            )
            raise

        clips_dump_path.unlink(missing_ok=True)

        write_failed_clips(stats["not_available"], failed_clips_path)

        file_size_MB = round(parquet_file_path.stat().st_size / (1024 * 1024), 1)
        not_available = len(stats["not_available"])
        result = {
            "parquet_path": str(parquet_file_path),
            "clip_count": len(clip_ids),
            "rows_written": rows_written,
            "top_level_fields": len(schema),
            "leaf_columns": leaf_columns,
            "row_groups": row_groups,
            "file_size_MB": file_size_MB,
            "not_available": not_available,
            "failed_clips_path": str(failed_clips_path),
            "levels": stats["levels"],
        }

        notify(
            "info",
            f"Abgeschlossen: {rows_written}/{len(clip_ids)} Clips, {leaf_columns} Blattspalten, "
            f"{row_groups} Blockgruppen, {file_size_MB} MB -> {parquet_file_path}",
            message_handler,
        )

        if not_available:
            notify("info", f"IDs nicht abrufbar: {not_available} (Liste: {failed_clips_path})",
                   message_handler)

        return result
    finally:
        release_lock()


def main(argv=None):
    """Standalone-Einstieg. Rueckgabe ist der Exit-Code (0 = ok)."""
    argv = list(sys.argv[1:] if argv is None else argv)
    extra_args = " ".join(argv) if argv else None
    try:
        update_metadata_file(extra_args=extra_args)
        return 0
    except Exception as exc:
        error_message = f"Unhandled error in main: {exc}"
        print(error_message, file=sys.stderr)
        try:
            tb_write_log(main_log, error_message)
            tb_write_log(main_log, traceback.format_exc())
        except Exception:
            pass
        return 1


# --------- EXEC ---------
if __name__ == "__main__":
    sys.exit(main())
