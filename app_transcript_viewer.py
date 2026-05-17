from pathlib import Path

import pandas as pd
import streamlit as st

DATASET_DIR = Path("/home/ivan/tmp/new/deep_learning/lab_3/Dataset")
CSV_PATH = DATASET_DIR / "transcriptions.csv"


def load_transcripts() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH)
    if "checked" not in df.columns:
        df["checked"] = False
    if "id" not in df.columns:
        df["id"] = df["wav_filename"].str.replace(".wav", "", regex=False)
    return df


def save_transcript(id: str, new_text: str, checked: bool) -> None:
    df = load_transcripts()
    df.loc[df["id"] == id, "text"] = new_text or ""
    df.loc[df["id"] == id, "checked"] = checked
    df.to_csv(CSV_PATH, index=False)


def save_checked(id: str, checked: bool) -> None:
    df = load_transcripts()
    df.loc[df["id"] == id, "checked"] = checked
    df.to_csv(CSV_PATH, index=False)


st.set_page_config(page_title="Transcript Viewer", layout="wide")
st.title("Audio Transcript Viewer")

df = load_transcripts()

if "selected_id" not in st.session_state:
    st.session_state.selected_id = None

col_list, col_detail = st.columns([1, 2])

with col_list:
    filter_option = st.selectbox("Filter", ["All", "Unchecked only"], index=0)

    if filter_option == "Unchecked only":
        filtered_df = df[df["checked"] == False]
    else:
        filtered_df = df

    st.subheader(f"Transcripts ({len(filtered_df)} of {len(df)} total)")
    with st.container(height=600):
        for _, row in filtered_df.iterrows():
            row_id: str = str(row["id"])
            checked_marker = "✅" if row.get("checked", False) else "⬜"
            label = f"{checked_marker} {row_id} — {str(row['text'])[:60]}..."
            if st.button(label, key=row_id, width="stretch"):
                st.session_state.selected_id = row_id

with col_detail:
    if st.session_state.selected_id:
        row = df[df["id"] == st.session_state.selected_id].iloc[0]
        row_id: str = str(row["id"])
        audio_path = DATASET_DIR / row["wav_filename"]
        transcript_val = row["text"]
        transcript: str = str(transcript_val) if transcript_val else ""
        is_checked: bool = bool(row.get("checked", False))

        st.subheader(f"File: {row_id}")

        current_checked = st.checkbox(
            "Checked",
            value=is_checked,
            key=f"checked_{row_id}",
        )
        if current_checked != is_checked:
            save_checked(row_id, current_checked)
            st.rerun()

        st.audio(str(audio_path), format="audio/wav")

        duration = row.get("seconds", 0)
        st.write(f"**Duration:** {duration:.2f}s")

        new_transcript = st.text_area(
            "Transcript",
            value=transcript,
            height=200,
            key=f"transcript_{row_id}",
        )

        if st.button("Save Changes", key=f"save_{row_id}"):
            save_transcript(row_id, new_transcript, current_checked)
            st.success("Saved!")
            st.rerun()
    else:
        st.info("Select an audio file from the list.")
