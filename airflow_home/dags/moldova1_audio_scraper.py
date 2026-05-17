import os
import re
import pendulum
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from airflow.sdk import dag, task

BASE_URL = "https://moldova1.md"
SHOW_ID = 59
DOWNLOAD_DIR = "/home/ivan/tmp/new/deep_learning/lab_3/Download"


@dag(
    dag_id="01_colectare_audio_moldova1_bash",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraping", "audio"],
)
def audio_collection_pipeline():

    # ─────────────────────────────────────────────────────────────
    # TASK 1: Descoperă toate paginile de catalog /w/ro/59/N
    # Returnează lista de URL-uri de pagini
    # ─────────────────────────────────────────────────────────────
    @task
    def discover_pages() -> list[str]:
        page_urls = []
        page_nr = 1
        while True:
            url = f"{BASE_URL}/w/ro/{SHOW_ID}/{page_nr}"
            print(f"Verific pagina catalog: {url}")
            try:
                resp = requests.get(url, timeout=15)
                if resp.status_code == 404:
                    print(f"Pagina {page_nr} returnează 404 — stop.")
                    break
                resp.raise_for_status()
            except requests.HTTPError:
                print(f"Eroare HTTP la pagina {page_nr} — stop.")
                break
            except Exception as e:
                print(f"Eroare la pagina {page_nr}: {e} — stop.")
                break

            # Verificăm dacă pagina conține episoade (link-uri /f/ro/)
            soup = BeautifulSoup(resp.text, "html.parser")
            episode_links = soup.select('a[href^="/f/ro/"]')
            if not episode_links:
                print(f"Pagina {page_nr} nu conține episoade — stop.")
                break

            page_urls.append(url)
            print(f"  → Pagina {page_nr} OK, {len(episode_links)} episoade găsite.")
            page_nr += 1

        print(f"Total pagini catalog descoperite: {len(page_urls)}")
        return page_urls

    # ─────────────────────────────────────────────────────────────
    # TASK 2: Din fiecare pagină catalog extrage link-urile episoadelor
    # Input:  URL pagină catalog  (ex: https://moldova1.md/w/ro/59/1)
    # Output: listă de URL-uri episoade (ex: https://moldova1.md/f/ro/8293)
    # ─────────────────────────────────────────────────────────────
    @task
    def extract_episode_links_from_page(page_url: str) -> list[str]:
        print(f"Extrag episoade din catalogul: {page_url}")
        try:
            resp = requests.get(page_url, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"Eroare la {page_url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        # Selectăm toate link-urile de forma /f/ro/{id}#content din div#list
        seen = set()
        episode_urls = []
        for a in soup.select('div#list a[href^="/f/ro/"]'):
            href = a["href"].split("#")[0]  # eliminăm #content
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen:
                seen.add(full_url)
                episode_urls.append(full_url)

        print(f"  → {len(episode_urls)} episoade găsite pe {page_url}")
        return episode_urls

    # ─────────────────────────────────────────────────────────────
    # TASK 3: Din pagina unui episod extrage URL-ul m3u8
    # Input:  URL episod  (ex: https://moldova1.md/f/ro/8293)
    # Output: dict cu {episode_url, m3u8_url} sau None dacă nu găsit
    # ─────────────────────────────────────────────────────────────
    @task
    def extract_m3u8_from_episode(episode_url: str) -> dict | None:
        print(f"Extrag m3u8 din episodul: {episode_url}")
        try:
            resp = requests.get(episode_url, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"Eroare la {episode_url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Căutăm <source src="..." type="application/x-mpegURL">
        source_tag = soup.find("source", {"type": "application/x-mpegURL"})
        if source_tag and source_tag.get("src"):
            m3u8_url = source_tag["src"]
            print(f"  → M3U8 găsit: {m3u8_url}")
            return {"episode_url": episode_url, "m3u8_url": m3u8_url}

        # Fallback: regex în HTML brut
        match = re.search(r"(https?://[^\s\"'<>]+?\.m3u8[^\s\"'<>]*)", resp.text)
        if match:
            m3u8_url = match.group(1)
            print(f"  → M3U8 găsit (regex fallback): {m3u8_url}")
            return {"episode_url": episode_url, "m3u8_url": m3u8_url}

        print(f"  → M3U8 NU a fost găsit pentru {episode_url}")
        return None

    # ─────────────────────────────────────────────────────────────
    # TASK 4: Descarcă audio din m3u8 cu ffmpeg (@task.bash)
    # Input:  dict {"episode_url": ..., "m3u8_url": ...}
    # ─────────────────────────────────────────────────────────────
    @task.bash
    def download_audio(episode_info: dict | None) -> str:
        if episode_info is None:
            return "echo 'Skipping: episode_info is None'"

        m3u8_url = episode_info["m3u8_url"]
        episode_url = episode_info["episode_url"]

        # Generăm numele fișierului din ID-ul episodului (ex: /f/ro/8293 → ep_8293.wav)
        episode_id = episode_url.rstrip("/").split("/")[-1]
        output_file = f"{DOWNLOAD_DIR}/ep_{episode_id}.wav"

        return (
            f'mkdir -p "{DOWNLOAD_DIR}" && '
            f'if [ -f "{output_file}" ]; then '
            f'  echo "Deja descărcat: {output_file}"; '
            f'else '
            f'  ffmpeg -i "{m3u8_url}" '
            f'    -vn -acodec pcm_s16le -ar 16000 -ac 1 '
            f'    "{output_file}" && '
            f'  echo "Descărcat: {output_file}"; '
            f'fi'
        )

    # ─────────────────────────────────────────────────────────────
    # ORCHESTRARE: conectăm taskurile
    # ─────────────────────────────────────────────────────────────
    pages = discover_pages()

    # Pentru fiecare pagină catalog → extrage episoadele (Dynamic Task Mapping)
    episode_links_per_page = extract_episode_links_from_page.expand(page_url=pages)

    # Aplatizăm lista de liste într-o singură listă de URL-uri
    @task
    def flatten_episode_links(nested: list[list[str]]) -> list[str]:
        flat = []
        for sublist in nested:
            flat.extend(sublist)
        # Eliminăm duplicate (un episod poate apărea pe mai multe pagini)
        unique = list(dict.fromkeys(flat))
        print(f"Total episoade unice: {len(unique)}")
        return unique

    all_episode_urls = flatten_episode_links(episode_links_per_page)

    # Pentru fiecare episod → extrage m3u8
    m3u8_infos = extract_m3u8_from_episode.expand(episode_url=all_episode_urls)

    # Pentru fiecare m3u8 → descarcă audio
    download_audio.expand(episode_info=m3u8_infos)


audio_collection_pipeline()
