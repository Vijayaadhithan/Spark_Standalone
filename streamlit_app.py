from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from local_app import (
    DatasetPathError,
    collect_dashboard_data,
    extract_dataset_archive,
    get_archive_dataset_dir,
    is_archive_path,
    resolve_dataset_dir,
)

DEFAULT_DATASET_SOURCE = "/Users/vjaadhi2799/Downloads/millionsongsubset.tar.gz"

st.set_page_config(
    page_title="Million Song Spark Explorer",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(show_spinner=False)
def load_dashboard(dataset_dir: str, driver_memory: str) -> dict[str, object]:
    return collect_dashboard_data(dataset_dir, driver_memory=driver_memory)


@st.cache_data(show_spinner=False)
def dataframe_to_csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(200, 92, 47, 0.14), transparent 28%),
                radial-gradient(circle at top right, rgba(42, 109, 104, 0.14), transparent 24%),
                linear-gradient(180deg, #f6efe4 0%, #f0e4d2 100%);
        }
        h1, h2, h3 {
            letter-spacing: -0.02em;
        }
        .hero {
            border: 1px solid rgba(29, 26, 23, 0.08);
            background: linear-gradient(135deg, rgba(255, 250, 244, 0.92), rgba(234, 223, 206, 0.88));
            padding: 1.75rem 1.9rem;
            border-radius: 22px;
            box-shadow: 0 20px 60px rgba(29, 26, 23, 0.07);
            margin-bottom: 1.2rem;
        }
        .hero-kicker {
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.8rem;
            color: #a14d28;
            margin-bottom: 0.4rem;
        }
        .hero-copy {
            font-size: 1.05rem;
            line-height: 1.65;
            max-width: 54rem;
            color: #3d3831;
        }
        .note-card {
            border-left: 4px solid #c85c2f;
            background: rgba(255, 251, 245, 0.86);
            padding: 1rem 1.1rem;
            border-radius: 14px;
            margin-bottom: 0.9rem;
        }
        .note-card.alt {
            border-left-color: #2a6d68;
        }
        .small-muted {
            color: #61584e;
            font-size: 0.92rem;
        }
        div[data-testid="stMetric"] {
            background: rgba(255, 252, 247, 0.86);
            border: 1px solid rgba(29, 26, 23, 0.07);
            padding: 0.8rem;
            border-radius: 16px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero(overview: dict[str, object], dataset_dir: str) -> None:
    min_year = overview["min_year"] or "N/A"
    max_year = overview["max_year"] or "N/A"
    ingestion_mode = overview.get("ingestion_mode", "spark_rdd")
    ingestion_copy = (
        "Spark reads the HDF5 file list in parallel."
        if ingestion_mode == "spark_rdd"
        else "HDF5 rows are read on the driver first, then handed to Spark as a DataFrame because this Python/PySpark combination cannot safely serialize Python RDD functions."
    )
    st.markdown(
        f"""
        <div class="hero">
            <div class="hero-kicker">Local Spark Application</div>
            <h1>Million Song Spark Explorer</h1>
            <p class="hero-copy">
                This app keeps Apache Spark as the processing engine, but replaces notebook tabs with a visible
                interface. It reads the Million Song Subset locally, turns the HDF5 files into a Spark DataFrame,
                and explains the analysis alongside the charts so the project feels like an application instead of a lab notebook.
            </p>
            <p class="small-muted">
                Dataset directory: {dataset_dir}<br>
                Coverage window: {min_year} to {max_year}<br>
                Ingestion mode: {ingestion_copy}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_story_tab(data: dict[str, object]) -> None:
    overview = data["overview"]
    yearly_peak = data["yearly_peak"]
    popular_by_year = data["popular_by_year"]

    left, right = st.columns([1.1, 0.9], gap="large")

    with left:
        st.markdown("### What This View Shows")
        st.markdown(
            """
            <div class="note-card">
                The original notebooks focused on song counts, song popularity, and changes over time. This app keeps
                the same questions but presents them as a narrative: what is in the dataset, which tracks rise to the top,
                and how the popularity-like score shifts across years.
            </div>
            """,
            unsafe_allow_html=True,
        )
        if not yearly_peak.empty:
            st.markdown("#### Peak Song Hotttnesss by Year")
            st.bar_chart(yearly_peak.set_index("year"))
        else:
            st.info("No year-level peak chart is available because the dataset did not expose usable year values.")

    with right:
        st.markdown("### Why Spark Still Matters")
        st.markdown(
            """
            <div class="note-card alt">
                Even on one machine, Spark is useful here because the source data is split across thousands of HDF5 files.
                The app uses Spark in <code>local[*]</code> mode to fan out file reads, normalize the records, and build a
                reusable DataFrame before the Streamlit layer renders tables and charts.
            </div>
            """,
            unsafe_allow_html=True,
        )
        if not popular_by_year.empty:
            st.markdown("#### Songs With Hotttnesss >= 0.5")
            st.line_chart(popular_by_year.set_index("year"))
        else:
            st.info("No popularity trend chart is available because the current dataset slice has sparse year coverage.")

    st.markdown("### Headline Rankings")
    rankings_left, rankings_right = st.columns(2, gap="large")

    with rankings_left:
        st.dataframe(
            data["top_songs"],
            use_container_width=True,
            hide_index=True,
            column_config={
                "song_hotttnesss": st.column_config.NumberColumn(format="%.3f"),
            },
        )

    with rankings_right:
        st.dataframe(
            data["top_artists"],
            use_container_width=True,
            hide_index=True,
            column_config={
                "peak_song_hotttnesss": st.column_config.NumberColumn(format="%.3f"),
            },
        )

    st.caption(
        f"Loaded {overview['song_count']:,} songs from {overview['file_count']:,} HDF5 files in "
        f"{overview['load_seconds']} seconds using Python {overview['runtime_python']} and PySpark {overview['runtime_pyspark']}."
    )


def build_filtered_artist_table(filtered: pd.DataFrame) -> pd.DataFrame:
    if filtered.empty:
        return pd.DataFrame(columns=["artist_name", "peak_song_hotttnesss", "track_count", "sample_titles"])

    scored = filtered.dropna(subset=["song_hotttnesss"]).copy()
    if scored.empty:
        return pd.DataFrame(columns=["artist_name", "peak_song_hotttnesss", "track_count", "sample_titles"])

    artists = (
        scored.groupby("artist_name", dropna=False)
        .agg(
            peak_song_hotttnesss=("song_hotttnesss", "max"),
            track_count=("song_id", "count"),
            sample_titles=(
                "title",
                lambda values: ", ".join(sorted(pd.unique([value for value in values if value]))[:5]),
            ),
        )
        .reset_index()
        .sort_values(["peak_song_hotttnesss", "track_count"], ascending=[False, False])
        .head(20)
    )
    artists["artist_name"] = artists["artist_name"].fillna("Unknown artist")
    artists["peak_song_hotttnesss"] = artists["peak_song_hotttnesss"].round(3)
    return artists


def render_audio_tab(data: dict[str, object]) -> None:
    audio_overview = data["audio_overview"]
    key_distribution = data["key_distribution"]
    mode_distribution = data["mode_distribution"]
    time_signature_distribution = data["time_signature_distribution"]
    tempo_bands = data["tempo_bands"]
    duration_bands = data["duration_bands"]
    top_fast_tracks = data["top_fast_tracks"]
    audio_scatter = data["audio_scatter"]

    st.markdown("### Audio Feature View")
    st.markdown(
        """
        <div class="note-card">
            This layer uses Spark to read the <code>analysis/songs</code> fields from the HDF5 files and surface
            musical structure signals such as tempo, duration, loudness, key, mode, and time signature.
        </div>
        <div class="note-card alt">
            The Million Song Subset does contain <code>danceability</code> and <code>energy</code> columns, but in this
            subset they are effectively constant zeros, so they are not useful for analysis.
        </div>
        """,
        unsafe_allow_html=True,
    )

    metric_cols = st.columns(5, gap="large")
    metric_cols[0].metric("Songs With Tempo", f"{audio_overview['songs_with_tempo']:,}")
    metric_cols[1].metric(
        "Median Tempo",
        f"{audio_overview['median_tempo']} BPM" if audio_overview["median_tempo"] is not None else "N/A",
    )
    metric_cols[2].metric(
        "Avg Duration",
        f"{round(audio_overview['avg_duration'] / 60, 2)} min" if audio_overview["avg_duration"] is not None else "N/A",
    )
    metric_cols[3].metric(
        "Avg Loudness",
        f"{audio_overview['avg_loudness']} dB" if audio_overview["avg_loudness"] is not None else "N/A",
    )
    metric_cols[4].metric(
        "Major Mode Share",
        f"{audio_overview['major_share']:.1%}" if audio_overview["major_share"] is not None else "N/A",
    )

    rhythm_left, rhythm_right = st.columns(2, gap="large")
    with rhythm_left:
        st.markdown("#### Tempo Bands")
        if not tempo_bands.empty:
            st.bar_chart(tempo_bands.set_index("tempo_band_label")[["song_count"]])
        else:
            st.info("Tempo values were not available for this dataset.")

    with rhythm_right:
        st.markdown("#### Duration Bands")
        if not duration_bands.empty:
            st.bar_chart(duration_bands.set_index("duration_band_label")[["song_count"]])
        else:
            st.info("Duration values were not available for this dataset.")

    harmony_left, harmony_mid, harmony_right = st.columns(3, gap="large")
    with harmony_left:
        st.markdown("#### Musical Key Distribution")
        if not key_distribution.empty:
            st.bar_chart(key_distribution.set_index("key_label")[["song_count"]])
        else:
            st.info("Key values were not available for this dataset.")

    with harmony_mid:
        st.markdown("#### Major vs Minor")
        if not mode_distribution.empty:
            st.bar_chart(mode_distribution.set_index("mode_label")[["song_count"]])
        else:
            st.info("Mode values were not available for this dataset.")

    with harmony_right:
        st.markdown("#### Time Signature")
        if not time_signature_distribution.empty:
            st.bar_chart(time_signature_distribution.set_index("time_signature_label")[["song_count"]])
        else:
            st.info("Time signature values were not available for this dataset.")

    st.markdown("#### Fastest Tracks In The Dataset")
    st.dataframe(
        top_fast_tracks,
        use_container_width=True,
        hide_index=True,
        column_config={
            "tempo": st.column_config.NumberColumn(format="%.1f"),
            "duration": st.column_config.NumberColumn(format="%.1f"),
            "song_hotttnesss": st.column_config.NumberColumn(format="%.3f"),
        },
    )

    scatter_options = {
        "Tempo vs Song Hotttnesss": "tempo",
        "Duration vs Song Hotttnesss": "duration",
        "Loudness vs Song Hotttnesss": "loudness",
    }
    scatter_choice = st.selectbox(
        "Compare Audio Feature Against Song Hotttnesss",
        list(scatter_options.keys()),
    )
    scatter_column = scatter_options[scatter_choice]
    scatter_view = audio_scatter.dropna(subset=[scatter_column, "song_hotttnesss"])
    if not scatter_view.empty:
        st.scatter_chart(scatter_view, x=scatter_column, y="song_hotttnesss")
        st.caption("Scatter view is sampled from the Spark DataFrame to keep the UI responsive.")
    else:
        st.info("Not enough data points were available for this comparison.")


def render_explore_tab(data: dict[str, object]) -> None:
    catalog = data["catalog"].copy()
    valid_years = catalog["year"].dropna()

    controls_left, controls_right, controls_search = st.columns([1, 1, 1.2], gap="large")
    if not valid_years.empty:
        year_range = controls_left.slider(
            "Year Range",
            min_value=int(valid_years.min()),
            max_value=int(valid_years.max()),
            value=(int(valid_years.min()), int(valid_years.max())),
        )
    else:
        year_range = None
        controls_left.info("Year filtering is unavailable because the dataset has no usable year values.")

    hotttnesss_floor = controls_right.slider(
        "Minimum Song Hotttnesss",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.05,
    )
    search_term = controls_search.text_input(
        "Artist Or Title Search",
        placeholder="Search the filtered view",
    ).strip()

    filtered = catalog.copy()
    if year_range is not None:
        filtered = filtered[filtered["year"].between(year_range[0], year_range[1], inclusive="both")]
    if hotttnesss_floor > 0:
        filtered = filtered[filtered["song_hotttnesss"].fillna(-1) >= hotttnesss_floor]
    if search_term:
        search_mask = filtered["artist_name"].str.contains(search_term, case=False, na=False) | filtered[
            "title"
        ].str.contains(search_term, case=False, na=False)
        filtered = filtered[search_mask]

    filtered_left, filtered_mid, filtered_right = st.columns(3, gap="large")
    filtered_left.metric("Visible Songs", f"{len(filtered):,}")
    filtered_mid.metric("Visible Artists", f"{filtered['artist_name'].nunique():,}")
    filtered_right.metric(
        "Avg Hotttnesss",
        f"{filtered['song_hotttnesss'].dropna().mean():.3f}" if filtered["song_hotttnesss"].notna().any() else "N/A",
    )

    export_left, export_right = st.columns(2, gap="large")
    export_left.download_button(
        "Download Filtered Songs As CSV",
        data=dataframe_to_csv_bytes(
            filtered[
                [
                    "song_id",
                    "title",
                    "artist_name",
                    "release",
                    "artist_location",
                    "year",
                    "song_hotttnesss",
                    "tempo",
                    "duration",
                    "loudness",
                    "key_label",
                    "mode_label",
                    "time_signature",
                ]
            ]
        ),
        file_name="million_song_filtered_catalog.csv",
        mime="text/csv",
        use_container_width=True,
    )

    filtered_songs = (
        filtered.dropna(subset=["song_hotttnesss"])
        .sort_values(["song_hotttnesss", "artist_name"], ascending=[False, True])
        .head(20)
    )
    filtered_artists = build_filtered_artist_table(filtered)
    export_right.download_button(
        "Download Filtered Artists As CSV",
        data=dataframe_to_csv_bytes(filtered_artists),
        file_name="million_song_filtered_artists.csv",
        mime="text/csv",
        use_container_width=True,
    )

    charts_left, charts_right = st.columns(2, gap="large")
    with charts_left:
        st.markdown("#### Filtered Top Songs")
        st.dataframe(
            filtered_songs[["title", "artist_name", "release", "year", "song_hotttnesss", "tempo", "duration"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "song_hotttnesss": st.column_config.NumberColumn(format="%.3f"),
                "tempo": st.column_config.NumberColumn(format="%.1f"),
                "duration": st.column_config.NumberColumn(format="%.1f"),
            },
        )

    with charts_right:
        st.markdown("#### Filtered Top Artists")
        st.dataframe(
            filtered_artists,
            use_container_width=True,
            hide_index=True,
            column_config={
                "peak_song_hotttnesss": st.column_config.NumberColumn(format="%.3f"),
            },
        )

    if not filtered.empty and filtered["year"].notna().any() and filtered["song_hotttnesss"].notna().any():
        year_view = (
            filtered.dropna(subset=["year", "song_hotttnesss"])
            .groupby("year", dropna=False)["song_hotttnesss"]
            .max()
            .sort_index()
            .to_frame("peak_song_hotttnesss")
        )
        st.markdown("#### Filtered Yearly Peaks")
        st.area_chart(year_view)

    st.markdown("#### Filtered Catalog")
    st.dataframe(
        filtered[
            [
                "title",
                "artist_name",
                "release",
                "artist_location",
                "year",
                "song_hotttnesss",
                "tempo",
                "duration_minutes",
                "loudness",
                "key_label",
                "mode_label",
                "time_signature",
            ]
        ].head(250),
        use_container_width=True,
        hide_index=True,
        column_config={
            "song_hotttnesss": st.column_config.NumberColumn(format="%.3f"),
            "tempo": st.column_config.NumberColumn(format="%.1f"),
            "duration_minutes": st.column_config.NumberColumn(format="%.2f"),
            "loudness": st.column_config.NumberColumn(format="%.1f"),
        },
    )


def render_methods_tab(data: dict[str, object]) -> None:
    overview = data["overview"]
    location_share = 0.0
    if overview["song_count"]:
        location_share = overview["songs_with_location"] / overview["song_count"]

    st.markdown("### Processing Pipeline")
    st.markdown(
        """
        1. Point the app to either the extracted dataset folder or the original `.tar.gz` archive.
        2. Extract once if needed, then read the HDF5 metadata, year, and audio-analysis fields.
        3. Build a Spark DataFrame in `local[*]` mode.
        4. Use Spark aggregations for the headline metrics and trend tables.
        5. Materialize the result tables for Streamlit and allow filtered CSV exports from the interactive view.
        6. Render the results in Streamlit with narrative notes, interactive filtering, and ranked views.
        """
    )

    st.markdown("### Execution Profile")
    if overview.get("spark_master", "").startswith("local["):
        workers_copy = (
            "This app is running in Spark local mode. That means there are no separate Spark Worker nodes; "
            "Spark uses local threads on this machine."
        )
    else:
        workers_copy = "This app is not running in local mode."

    ingest_copy = (
        f"HDF5 file ingestion is distributed across {overview['ingest_partitions']} Spark partitions/tasks."
        if overview.get("ingestion_mode") == "spark_rdd"
        else "HDF5 file ingestion is happening on the driver first, then the rows are turned into a Spark DataFrame."
    )

    st.markdown(
        f"""
        - Spark master: `{overview['spark_master']}`
        - Local CPU cores visible to Python: `{overview['cpu_count']}`
        - Spark default parallelism: `{overview['default_parallelism']}`
        - Spark shuffle partitions: `{overview['shuffle_partitions']}`
        - HDF5 ingestion partitions: `{overview['ingest_partitions']}`
        """
    )
    st.caption(f"{workers_copy} {ingest_copy}")

    st.markdown("### Cluster Option")
    st.markdown(
        """
        If you want to demonstrate a real Spark standalone setup instead of local mode, run a local Spark master and
        one or more worker processes, then start the app with `SPARK_MASTER_URL=spark://localhost:7077`.
        Splitting one machine into multiple Spark workers is mainly useful for showing the Spark architecture and UI;
        it usually does not make a single-machine workload faster than `local[*]`.
        """
    )
    st.code(
        "./scripts/start-standalone-cluster.sh\n"
        "export SPARK_MASTER_URL=spark://127.0.0.1:7077\n"
        "export SPARK_EXECUTOR_INSTANCES=2\n"
        "export SPARK_EXECUTOR_CORES=3\n"
        "export SPARK_EXECUTOR_MEMORY=2G\n"
        "streamlit run streamlit_app.py",
        language="bash",
    )

    if overview.get("ingestion_mode") == "driver_fallback":
        st.warning(
            "This session is using driver-side HDF5 ingestion because Python "
            f"{overview['runtime_python']} with PySpark {overview['runtime_pyspark']} cannot safely serialize "
            "Python RDD functions in this code path. The analytics still run in Spark after the DataFrame is created."
        )

    st.markdown("### Spark Concepts Used Here")
    st.markdown(
        """
        - `SparkSession`: the entrypoint for running Spark locally inside the app.
        - `local[*]` execution: uses all local CPU cores instead of a remote Spark cluster.
        - `RDD partitioning`: the HDF5 file list is parallelized so Spark can read many files concurrently.
        - `Schema-driven DataFrame creation`: the raw records are normalized into a typed Spark DataFrame.
        - `Transformations`: filtering, de-duplication, grouping, sorting, and column cleanup happen lazily in Spark.
        - `Actions`: `count()` and `toPandas()` trigger actual execution when the app needs metrics or tables.
        - `Caching`: the shared Spark DataFrame is cached because multiple charts and tables reuse it.
        - `Aggregations`: yearly peaks, popular-song counts, and artist rankings are all Spark group-by aggregations.
        - `Shuffle awareness`: grouped and sorted results are the expensive part, so the app keeps the analysis surface focused.
        - `Driver boundary`: Streamlit only receives the smaller result tables, not the entire raw dataset UI-side.
        """
    )
    st.caption(
        "This does not try to cover every Spark topic. It focuses on the concepts that are actually useful for this local analytics application."
    )

    methods_left, methods_right = st.columns(2, gap="large")

    with methods_left:
        st.markdown("### Interpretation Notes")
        st.markdown(
            """
            <div class="note-card">
                <strong>Song hotttnesss</strong> is a score already embedded in the dataset. The app treats it as a
                popularity-like signal for ranking and trend analysis.
            </div>
            <div class="note-card alt">
                Missing fields are normal in this dataset. Year and location coverage are incomplete, so the app keeps
                those gaps visible instead of silently filling them with invented values.
            </div>
            <div class="note-card">
                The primary pipeline is <strong>HDF5 -&gt; Spark DataFrame</strong>. CSV is used as an export format for
                filtered results, not as the core storage format for the whole dataset. If we want a persistent intermediate
                dataset later, <strong>Parquet</strong> is the better Spark-native choice.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with methods_right:
        st.markdown("### Dataset Coverage")
        st.markdown(
            f"""
            <div class="note-card">
                <strong>Songs with year:</strong> {overview['songs_with_year']:,} of {overview['song_count']:,}<br>
                <strong>Songs with location:</strong> {overview['songs_with_location']:,} of {overview['song_count']:,}<br>
                <strong>Location coverage:</strong> {location_share:.1%}
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("### Local Setup Notes")
    st.code(
        "python3.11 -m venv .venv\n"
        "source .venv/bin/activate\n"
        "pip install -r requirements.txt\n"
        "streamlit run streamlit_app.py",
        language="bash",
    )


def main() -> None:
    inject_styles()

    dataset_default = os.environ.get("MSD_DATASET_SOURCE", DEFAULT_DATASET_SOURCE)
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
    extract_root_default = str((Path.cwd() / "data").resolve())

    with st.sidebar:
        st.markdown("## Local Data Source")
        dataset_source = st.text_input(
            "Dataset Folder Or Archive",
            value=dataset_default,
            help="Use the extracted MillionSongSubset folder or the .tar.gz archive.",
        ).strip()
        extract_root = st.text_input(
            "Extraction Folder",
            value=extract_root_default,
            help="Used only when the source path is a tar archive.",
        ).strip()
        st.caption(f"Spark driver memory: {driver_memory}")

    if not dataset_source:
        st.markdown(
            """
            <div class="hero">
                <div class="hero-kicker">Setup</div>
                <h1>Point The App At Your Dataset</h1>
                <p class="hero-copy">
                    Add the extracted MillionSongSubset folder or the <code>millionsongsubset.tar.gz</code> archive in the
                    sidebar. The app will run Spark locally and turn the notebook analysis into an interactive view.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.stop()

    source_path = Path(dataset_source).expanduser()

    try:
        if is_archive_path(source_path):
            if not source_path.is_file():
                raise DatasetPathError(f"Archive path does not exist: {source_path}")

            extracted_dir = get_archive_dataset_dir(source_path, extract_root)
            if extracted_dir.exists() and any(extracted_dir.rglob("*.h5")):
                dataset_dir = resolve_dataset_dir(extracted_dir)
                st.sidebar.success(f"Using extracted dataset at {dataset_dir}")
            else:
                st.sidebar.warning("Archive detected. Extract it once before running the analysis.")
                if st.sidebar.button("Extract Archive To Local Data Folder", use_container_width=True):
                    with st.spinner("Extracting archive..."):
                        extract_dataset_archive(source_path, extract_root)
                    st.rerun()
                st.stop()
        else:
            dataset_dir = resolve_dataset_dir(source_path)
    except DatasetPathError as exc:
        st.error(str(exc))
        st.stop()

    with st.spinner("Running Spark locally and preparing the Streamlit view..."):
        data = load_dashboard(str(dataset_dir), driver_memory)

    render_hero(data["overview"], data["dataset_dir"])

    metrics = data["overview"]
    metric_cols = st.columns(4, gap="large")
    metric_cols[0].metric("Songs Ingested", f"{metrics['song_count']:,}")
    metric_cols[1].metric("Distinct Artists", f"{metrics['artist_count']:,}")
    metric_cols[2].metric("Songs With Year", f"{metrics['songs_with_year']:,}")
    metric_cols[3].metric("Spark Load Time", f"{metrics['load_seconds']} s")

    tab_story, tab_audio, tab_explore, tab_methods = st.tabs(["Story", "Audio", "Explore", "Method"])
    with tab_story:
        render_story_tab(data)
    with tab_audio:
        render_audio_tab(data)
    with tab_explore:
        render_explore_tab(data)
    with tab_methods:
        render_methods_tab(data)


if __name__ == "__main__":
    main()
