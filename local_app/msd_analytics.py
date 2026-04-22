import math
import os
import sys
import tarfile
import time
from pathlib import Path
from typing import Iterable
import zipfile

import h5py
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

KEY_LABELS = {
    0: "C",
    1: "C#/Db",
    2: "D",
    3: "D#/Eb",
    4: "E",
    5: "F",
    6: "F#/Gb",
    7: "G",
    8: "G#/Ab",
    9: "A",
    10: "A#/Bb",
    11: "B",
}
MODE_LABELS = {0: "Minor", 1: "Major"}

SONG_SCHEMA = T.StructType(
    [
        T.StructField("song_id", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("artist_id", T.StringType(), True),
        T.StructField("artist_name", T.StringType(), True),
        T.StructField("release", T.StringType(), True),
        T.StructField("artist_location", T.StringType(), True),
        T.StructField("song_hotttnesss", T.DoubleType(), True),
        T.StructField("year", T.IntegerType(), True),
        T.StructField("artist_familiarity", T.DoubleType(), True),
        T.StructField("artist_hotttnesss", T.DoubleType(), True),
        T.StructField("tempo", T.DoubleType(), True),
        T.StructField("duration", T.DoubleType(), True),
        T.StructField("loudness", T.DoubleType(), True),
        T.StructField("key", T.IntegerType(), True),
        T.StructField("mode", T.IntegerType(), True),
        T.StructField("time_signature", T.IntegerType(), True),
        T.StructField("source_file", T.StringType(), True),
    ]
)


class DatasetPathError(ValueError):
    pass


def is_archive_path(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".tar.gz") or name.endswith(".tgz") or name.endswith(".tar")


def _discover_h5_files(dataset_dir: Path) -> list[str]:
    return sorted(str(path) for path in dataset_dir.rglob("*.h5"))


def _major_minor(version_text: str) -> tuple[int, int]:
    parts = version_text.split(".")
    return int(parts[0]), int(parts[1])


def _requires_local_ingest_fallback() -> bool:
    if os.environ.get("MSD_FORCE_LOCAL_INGEST") == "1":
        return True

    # Spark 3.5.x on Python 3.14 hits cloudpickle serialization failures when
    # a Python function is shipped for RDD execution. Keep the analytics in Spark,
    # but build the initial rows on the driver in that environment.
    return sys.version_info >= (3, 14) and _major_minor(pyspark.__version__) < (4, 1)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _ensure_executor_pyfile() -> str:
    repo_root = _repo_root()
    package_root = repo_root / "local_app"
    pyfiles_dir = repo_root / ".spark-local" / "pyfiles"
    pyfiles_dir.mkdir(parents=True, exist_ok=True)
    pyfile_path = pyfiles_dir / "local_app.zip"

    with zipfile.ZipFile(pyfile_path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(package_root.rglob("*.py")):
            bundle.write(path, path.relative_to(repo_root))

    return str(pyfile_path)


def resolve_dataset_dir(source: str | Path) -> Path:
    path = Path(source).expanduser().resolve()
    if not path.exists():
        raise DatasetPathError(f"Dataset path does not exist: {path}")

    candidates = [path]
    if path.is_dir():
        candidates.append(path / "MillionSongSubset")

    for candidate in candidates:
        if candidate.is_dir() and _discover_h5_files(candidate):
            return candidate

    raise DatasetPathError(
        "Could not find any .h5 files under the provided dataset path. "
        "Point the app to the extracted MillionSongSubset directory."
    )


def _infer_archive_root_name(archive_path: Path) -> str:
    with tarfile.open(archive_path) as archive:
        for member in archive.getmembers():
            parts = Path(member.name).parts
            if parts:
                return parts[0]

    raise DatasetPathError(f"Archive is empty: {archive_path}")


def get_archive_dataset_dir(archive_path: str | Path, destination_root: str | Path) -> Path:
    archive = Path(archive_path).expanduser().resolve()
    root = Path(destination_root).expanduser().resolve()
    return root / _infer_archive_root_name(archive)


def _safe_extract(archive: tarfile.TarFile, destination_root: Path) -> None:
    destination_root = destination_root.resolve()
    root_prefix = f"{destination_root}{os.sep}"

    for member in archive.getmembers():
        target_path = (destination_root / member.name).resolve()
        if target_path != destination_root and not str(target_path).startswith(root_prefix):
            raise DatasetPathError("Archive contains an unsafe path and cannot be extracted.")

    archive.extractall(destination_root)


def extract_dataset_archive(archive_path: str | Path, destination_root: str | Path) -> Path:
    archive = Path(archive_path).expanduser().resolve()
    if not archive.is_file():
        raise DatasetPathError(f"Archive path does not exist: {archive}")

    destination = Path(destination_root).expanduser().resolve()
    dataset_dir = get_archive_dataset_dir(archive, destination)

    if dataset_dir.exists() and _discover_h5_files(dataset_dir):
        return dataset_dir

    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive) as handle:
        _safe_extract(handle, destination)

    return resolve_dataset_dir(dataset_dir)


def _normalize_scalar(value):
    if hasattr(value, "item"):
        value = value.item()

    if isinstance(value, bytes):
        value = value.decode("utf-8", "replace")

    if isinstance(value, str):
        value = value.replace("\x00", "").strip()
        return value or None

    if isinstance(value, float) and math.isnan(value):
        return None

    return value


def _normalize_float(value):
    normalized = _normalize_scalar(value)
    if normalized is None:
        return None
    return float(normalized)


def _normalize_int(value):
    normalized = _normalize_scalar(value)
    if normalized is None:
        return None
    return int(normalized)


def _round_or_none(value, digits=2):
    if value is None:
        return None
    return round(float(value), digits)


def _key_label(value) -> str:
    if pd.isna(value):
        return "Unknown"
    return KEY_LABELS.get(int(value), "Unknown")


def _mode_label(value) -> str:
    if pd.isna(value):
        return "Unknown"
    return MODE_LABELS.get(int(value), "Unknown")


def _tempo_band_label(start) -> str:
    return f"{int(start)}-{int(start) + 19} BPM"


def _duration_band_label(start_seconds) -> str:
    start_minutes = int(start_seconds // 60)
    return f"{start_minutes}-{start_minutes + 1} min"


def _read_song_records(paths):
    for file_path in paths:
        try:
            with h5py.File(file_path, "r") as handle:
                metadata = handle["/metadata/songs"][0]
                analysis = handle["/analysis/songs"][0]
                musicbrainz = handle.get("/musicbrainz/songs")
                year_value = None
                if musicbrainz is not None and len(musicbrainz):
                    year_value = _normalize_scalar(musicbrainz["year"][0])

                year = None
                if year_value:
                    year = int(year_value)
                    if year <= 0:
                        year = None

                yield (
                    _normalize_scalar(metadata["song_id"]),
                    _normalize_scalar(metadata["title"]),
                    _normalize_scalar(metadata["artist_id"]),
                    _normalize_scalar(metadata["artist_name"]),
                    _normalize_scalar(metadata["release"]),
                    _normalize_scalar(metadata["artist_location"]),
                    _normalize_float(metadata["song_hotttnesss"]),
                    year,
                    _normalize_float(metadata["artist_familiarity"]),
                    _normalize_float(metadata["artist_hotttnesss"]),
                    _normalize_float(analysis["tempo"]),
                    _normalize_float(analysis["duration"]),
                    _normalize_float(analysis["loudness"]),
                    _normalize_int(analysis["key"]),
                    _normalize_int(analysis["mode"]),
                    _normalize_int(analysis["time_signature"]),
                    file_path,
                )
        except (KeyError, OSError, ValueError):
            continue


def _get_spark_session(driver_memory: str) -> SparkSession:
    spark_master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    pyspark_python = os.environ.get("PYSPARK_PYTHON", sys.executable)
    pyspark_driver_python = os.environ.get("PYSPARK_DRIVER_PYTHON", sys.executable)
    executor_pyfile = _ensure_executor_pyfile()
    builder = (
        SparkSession.builder.master(spark_master)
        .appName("million-song-streamlit")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", max(4, os.cpu_count() or 4))
        .config("spark.pyspark.python", pyspark_python)
        .config("spark.pyspark.driver.python", pyspark_driver_python)
        .config("spark.submit.pyFiles", executor_pyfile)
        .config("spark.ui.showConsoleProgress", "false")
    )

    optional_env_configs = {
        "SPARK_EXECUTOR_MEMORY": "spark.executor.memory",
        "SPARK_EXECUTOR_CORES": "spark.executor.cores",
        "SPARK_CORES_MAX": "spark.cores.max",
        "SPARK_EXECUTOR_INSTANCES": "spark.executor.instances",
        "SPARK_DEFAULT_PARALLELISM": "spark.default.parallelism",
    }
    for env_name, spark_key in optional_env_configs.items():
        value = os.environ.get(env_name)
        if value:
            builder = builder.config(spark_key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.addPyFile(executor_pyfile)
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _format_catalog(frame: pd.DataFrame) -> pd.DataFrame:
    catalog = frame.copy()
    catalog["title"] = catalog["title"].fillna("Untitled")
    catalog["artist_name"] = catalog["artist_name"].fillna("Unknown artist")
    catalog["release"] = catalog["release"].fillna("Unknown release")
    catalog["artist_location"] = catalog["artist_location"].fillna("Unknown location")
    catalog["song_hotttnesss"] = catalog["song_hotttnesss"].round(3)
    catalog["tempo"] = catalog["tempo"].round(1)
    catalog["duration"] = catalog["duration"].round(1)
    catalog["duration_minutes"] = (catalog["duration"] / 60).round(2)
    catalog["loudness"] = catalog["loudness"].round(1)
    catalog["key_label"] = catalog["key"].apply(_key_label)
    catalog["mode_label"] = catalog["mode"].apply(_mode_label)
    return catalog


def _build_songs_dataframe(spark: SparkSession, h5_files: list[str]) -> tuple[object, str]:
    if _requires_local_ingest_fallback():
        rows = list(_read_song_records(h5_files))
        return spark.createDataFrame(rows, SONG_SCHEMA), "driver_fallback"

    partitions = min(len(h5_files), os.cpu_count() or 4)
    songs_rdd = spark.sparkContext.parallelize(h5_files, numSlices=max(1, partitions))
    return spark.createDataFrame(songs_rdd.mapPartitions(_read_song_records), SONG_SCHEMA), "spark_rdd"


def collect_dashboard_data(dataset_dir: str | Path, driver_memory: str = "4g") -> dict[str, object]:
    resolved_dir = resolve_dataset_dir(dataset_dir)
    h5_files = _discover_h5_files(resolved_dir)
    if not h5_files:
        raise DatasetPathError(f"No HDF5 files found in {resolved_dir}")

    start_time = time.perf_counter()
    spark = _get_spark_session(driver_memory)
    songs_df, ingestion_mode = _build_songs_dataframe(spark, h5_files)
    songs_df = songs_df.dropDuplicates(["song_id"]).cache()
    ingested_songs = songs_df.count()

    if ingested_songs == 0:
        raise DatasetPathError("Spark did not load any songs from the dataset.")

    overview_row = (
        songs_df.agg(
            F.count("*").alias("song_count"),
            F.countDistinct("artist_id").alias("artist_count"),
            F.sum(F.when(F.col("song_hotttnesss").isNotNull(), 1).otherwise(0)).alias(
                "songs_with_hotness"
            ),
            F.sum(F.when(F.col("year").isNotNull(), 1).otherwise(0)).alias("songs_with_year"),
            F.sum(F.when(F.col("artist_location").isNotNull(), 1).otherwise(0)).alias(
                "songs_with_location"
            ),
            F.min("year").alias("min_year"),
            F.max("year").alias("max_year"),
        ).first()
    )

    top_songs = (
        songs_df.filter(F.col("song_hotttnesss").isNotNull())
        .select("title", "artist_name", "release", "year", "song_hotttnesss")
        .orderBy(F.desc("song_hotttnesss"), F.asc("artist_name"))
        .limit(20)
        .toPandas()
    )

    top_artists = (
        songs_df.filter(F.col("song_hotttnesss").isNotNull())
        .withColumn("artist_name_clean", F.coalesce(F.col("artist_name"), F.lit("Unknown artist")))
        .withColumn("title_clean", F.coalesce(F.col("title"), F.lit("Untitled")))
        .groupBy("artist_name_clean")
        .agg(
            F.max("song_hotttnesss").alias("peak_song_hotttnesss"),
            F.count("*").alias("track_count"),
            F.slice(F.array_sort(F.collect_set("title_clean")), 1, 5).alias("sample_titles"),
        )
        .orderBy(F.desc("peak_song_hotttnesss"), F.desc("track_count"))
        .limit(20)
        .toPandas()
        .rename(columns={"artist_name_clean": "artist_name"})
    )

    yearly_peak = (
        songs_df.filter(F.col("year").isNotNull() & F.col("song_hotttnesss").isNotNull())
        .groupBy("year")
        .agg(F.max("song_hotttnesss").alias("peak_song_hotttnesss"))
        .orderBy("year")
        .toPandas()
    )

    popular_by_year = (
        songs_df.filter(
            F.col("year").isNotNull()
            & F.col("song_hotttnesss").isNotNull()
            & (F.col("song_hotttnesss") >= 0.5)
        )
        .groupBy("year")
        .agg(F.count("*").alias("popular_song_count"))
        .orderBy("year")
        .toPandas()
    )

    audio_overview_row = (
        songs_df.agg(
            F.sum(F.when(F.col("tempo").isNotNull() & (F.col("tempo") > 0), 1).otherwise(0)).alias(
                "songs_with_tempo"
            ),
            F.sum(
                F.when(F.col("duration").isNotNull() & (F.col("duration") > 0), 1).otherwise(0)
            ).alias("songs_with_duration"),
            F.sum(F.when(F.col("loudness").isNotNull(), 1).otherwise(0)).alias("songs_with_loudness"),
            F.expr("percentile_approx(tempo, 0.5)").alias("median_tempo"),
            F.expr("percentile_approx(duration, 0.5)").alias("median_duration"),
            F.avg(F.when(F.col("tempo") > 0, F.col("tempo"))).alias("avg_tempo"),
            F.avg(F.when(F.col("duration") > 0, F.col("duration"))).alias("avg_duration"),
            F.avg("loudness").alias("avg_loudness"),
            F.sum(F.when(F.col("mode") == 1, 1).otherwise(0)).alias("major_mode_count"),
            F.sum(F.when(F.col("mode") == 0, 1).otherwise(0)).alias("minor_mode_count"),
        ).first()
    )

    key_distribution = (
        songs_df.filter(F.col("key").isNotNull())
        .groupBy("key")
        .agg(F.count("*").alias("song_count"))
        .orderBy(F.desc("song_count"))
        .toPandas()
    )

    mode_distribution = (
        songs_df.filter(F.col("mode").isNotNull())
        .groupBy("mode")
        .agg(F.count("*").alias("song_count"))
        .orderBy(F.desc("song_count"))
        .toPandas()
    )

    time_signature_distribution = (
        songs_df.filter(F.col("time_signature").isNotNull())
        .groupBy("time_signature")
        .agg(F.count("*").alias("song_count"))
        .orderBy("time_signature")
        .toPandas()
    )

    tempo_bands = (
        songs_df.filter(F.col("tempo").isNotNull() & (F.col("tempo") > 0))
        .withColumn("tempo_band_start", (F.floor(F.col("tempo") / 20) * 20).cast("int"))
        .groupBy("tempo_band_start")
        .agg(
            F.count("*").alias("song_count"),
            F.avg("song_hotttnesss").alias("avg_song_hotttnesss"),
        )
        .orderBy("tempo_band_start")
        .toPandas()
    )

    duration_bands = (
        songs_df.filter(F.col("duration").isNotNull() & (F.col("duration") > 0))
        .withColumn("duration_band_start", (F.floor(F.col("duration") / 60) * 60).cast("int"))
        .groupBy("duration_band_start")
        .agg(
            F.count("*").alias("song_count"),
            F.avg("song_hotttnesss").alias("avg_song_hotttnesss"),
        )
        .orderBy("duration_band_start")
        .toPandas()
    )

    top_fast_tracks = (
        songs_df.filter(F.col("tempo").isNotNull() & (F.col("tempo") > 0))
        .select("title", "artist_name", "release", "tempo", "duration", "song_hotttnesss")
        .orderBy(F.desc("tempo"), F.asc("artist_name"))
        .limit(20)
        .toPandas()
    )

    audio_scatter = (
        songs_df.filter(F.col("song_hotttnesss").isNotNull())
        .filter(
            F.col("tempo").isNotNull() | F.col("duration").isNotNull() | F.col("loudness").isNotNull()
        )
        .sample(False, 0.35, seed=7)
        .select("title", "artist_name", "tempo", "duration", "loudness", "song_hotttnesss")
        .limit(1500)
        .toPandas()
    )

    catalog = (
        songs_df.select(
            "song_id",
            "title",
            "artist_name",
            "release",
            "artist_location",
            "year",
            "song_hotttnesss",
            "artist_familiarity",
            "artist_hotttnesss",
            "tempo",
            "duration",
            "loudness",
            "key",
            "mode",
            "time_signature",
        )
        .orderBy(F.desc_nulls_last("song_hotttnesss"), F.asc_nulls_last("year"))
        .toPandas()
    )

    songs_df.unpersist()

    if not top_songs.empty:
        top_songs["title"] = top_songs["title"].fillna("Untitled")
        top_songs["artist_name"] = top_songs["artist_name"].fillna("Unknown artist")
        top_songs["release"] = top_songs["release"].fillna("Unknown release")
        top_songs["song_hotttnesss"] = top_songs["song_hotttnesss"].round(3)

    if not top_artists.empty:
        top_artists["peak_song_hotttnesss"] = top_artists["peak_song_hotttnesss"].round(3)
        top_artists["sample_titles"] = top_artists["sample_titles"].apply(
            lambda values: ", ".join(values) if isinstance(values, list) else ""
        )

    if not yearly_peak.empty:
        yearly_peak["peak_song_hotttnesss"] = yearly_peak["peak_song_hotttnesss"].round(3)

    if not key_distribution.empty:
        key_distribution["key_label"] = key_distribution["key"].apply(_key_label)

    if not mode_distribution.empty:
        mode_distribution["mode_label"] = mode_distribution["mode"].apply(_mode_label)

    if not time_signature_distribution.empty:
        time_signature_distribution["time_signature_label"] = time_signature_distribution["time_signature"].apply(
            lambda value: f"{int(value)}/4"
        )

    if not tempo_bands.empty:
        tempo_bands["tempo_band_label"] = tempo_bands["tempo_band_start"].apply(_tempo_band_label)
        tempo_bands["avg_song_hotttnesss"] = tempo_bands["avg_song_hotttnesss"].round(3)

    if not duration_bands.empty:
        duration_bands["duration_band_label"] = duration_bands["duration_band_start"].apply(
            _duration_band_label
        )
        duration_bands["avg_song_hotttnesss"] = duration_bands["avg_song_hotttnesss"].round(3)

    if not top_fast_tracks.empty:
        top_fast_tracks["title"] = top_fast_tracks["title"].fillna("Untitled")
        top_fast_tracks["artist_name"] = top_fast_tracks["artist_name"].fillna("Unknown artist")
        top_fast_tracks["release"] = top_fast_tracks["release"].fillna("Unknown release")
        top_fast_tracks["tempo"] = top_fast_tracks["tempo"].round(1)
        top_fast_tracks["duration"] = top_fast_tracks["duration"].round(1)
        top_fast_tracks["song_hotttnesss"] = top_fast_tracks["song_hotttnesss"].round(3)

    if not audio_scatter.empty:
        audio_scatter["title"] = audio_scatter["title"].fillna("Untitled")
        audio_scatter["artist_name"] = audio_scatter["artist_name"].fillna("Unknown artist")
        audio_scatter["tempo"] = audio_scatter["tempo"].round(1)
        audio_scatter["duration"] = audio_scatter["duration"].round(1)
        audio_scatter["loudness"] = audio_scatter["loudness"].round(1)
        audio_scatter["song_hotttnesss"] = audio_scatter["song_hotttnesss"].round(3)

    catalog = _format_catalog(catalog)
    load_seconds = round(time.perf_counter() - start_time, 2)
    cpu_count = os.cpu_count() or 1
    ingest_partitions = min(len(h5_files), cpu_count) if ingestion_mode == "spark_rdd" else 0
    major_minor_total = int(audio_overview_row["major_mode_count"]) + int(audio_overview_row["minor_mode_count"])

    audio_overview = {
        "songs_with_tempo": int(audio_overview_row["songs_with_tempo"]),
        "songs_with_duration": int(audio_overview_row["songs_with_duration"]),
        "songs_with_loudness": int(audio_overview_row["songs_with_loudness"]),
        "median_tempo": _round_or_none(audio_overview_row["median_tempo"], 1),
        "avg_tempo": _round_or_none(audio_overview_row["avg_tempo"], 1),
        "median_duration": _round_or_none(audio_overview_row["median_duration"], 1),
        "avg_duration": _round_or_none(audio_overview_row["avg_duration"], 1),
        "avg_loudness": _round_or_none(audio_overview_row["avg_loudness"], 1),
        "major_share": round(
            int(audio_overview_row["major_mode_count"]) / major_minor_total, 3
        )
        if major_minor_total
        else None,
    }

    overview = {
        "song_count": int(overview_row["song_count"]),
        "artist_count": int(overview_row["artist_count"]),
        "songs_with_hotness": int(overview_row["songs_with_hotness"]),
        "songs_with_year": int(overview_row["songs_with_year"]),
        "songs_with_location": int(overview_row["songs_with_location"]),
        "min_year": int(overview_row["min_year"]) if overview_row["min_year"] is not None else None,
        "max_year": int(overview_row["max_year"]) if overview_row["max_year"] is not None else None,
        "file_count": len(h5_files),
        "load_seconds": load_seconds,
        "ingestion_mode": ingestion_mode,
        "runtime_python": ".".join(str(part) for part in sys.version_info[:3]),
        "runtime_pyspark": pyspark.__version__,
        "spark_master": spark.sparkContext.master,
        "default_parallelism": int(spark.sparkContext.defaultParallelism),
        "cpu_count": int(cpu_count),
        "shuffle_partitions": max(4, cpu_count),
        "ingest_partitions": int(ingest_partitions),
    }

    return {
        "dataset_dir": str(resolved_dir),
        "overview": overview,
        "catalog": catalog,
        "top_songs": top_songs,
        "top_artists": top_artists,
        "yearly_peak": yearly_peak,
        "popular_by_year": popular_by_year,
        "audio_overview": audio_overview,
        "key_distribution": key_distribution,
        "mode_distribution": mode_distribution,
        "time_signature_distribution": time_signature_distribution,
        "tempo_bands": tempo_bands,
        "duration_bands": duration_bands,
        "top_fast_tracks": top_fast_tracks,
        "audio_scatter": audio_scatter,
    }
