#!/usr/bin/env python3
import os
import re
import time
import asyncio
import aiohttp
import threading
import logging
import hashlib
import subprocess
import shutil
import platform
from tkinter import filedialog, messagebox, ttk
import tkinter as tk
from PIL import Image, ImageTk
from urllib.parse import urljoin
from bs4 import BeautifulSoup

#############################
# HELPER FUNCTIONS
#############################
def safe_filename(filename):
    """Return a sanitized version of filename containing only allowed characters."""
    return re.sub(r'[^\w\-.]', '_', filename)

def compute_md5(filepath, chunk_size=8192):
    """Compute the MD5 hash of a file in a memory‐efficient manner."""
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logging.error(f"Error computing MD5 for {filepath}: {e}")
        return None

#############################
# DOWNLOADER MODULE (Async)
#############################
class Downloader:
    def __init__(self, log_callback=None, progress_callback=None, gallery_callback=None):
        self.log_callback = log_callback
        self.progress_callback = progress_callback
        self.gallery_callback = gallery_callback
        self.session = None  # will be created lazily

    async def _create_session(self):
        headers = {'User-Agent': 'MyDownloader/1.0 (https://example.com)'}
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout, raise_for_status=False)

    async def close(self):
        if self.session:
            await self.session.close()

    async def download_series(self, url, folder, max_simultaneous, speed_limit, cancellation_event):
        if not self.session:
            await self._create_session()
        # --- Series parsing (unchanged) ---
        base_url, sep, qs = url.partition('?')
        qs = f'?{qs}' if qs else ""
        match = re.search(r"^(.*?)(\d+)(\.[^.]+)$", base_url)
        if not match:
            self._log("ERROR: URL does not end with a numeric sequence plus extension.")
            return
        prefix, number_str, suffix = match.groups()
        try:
            start_index = int(number_str)
        except ValueError:
            self._log("ERROR: Cannot interpret the numeric part of the URL.")
            return
        num_digits = len(number_str)
        self._log(f"Detected series starting at index: {start_index} (padding width: {num_digits})")
        semaphore = asyncio.Semaphore(max_simultaneous)

        async def try_download(i):
            if cancellation_event.is_set():
                return "CANCELLED"
            candidate1 = f"{prefix}{i}{suffix}"
            candidate2 = None
            if len(str(i)) < num_digits:
                candidate2 = f"{prefix}{str(i).zfill(num_digits)}{suffix}"
            candidates = [candidate1]
            if qs:
                candidates.append(candidate1 + qs)
            if candidate2 is not None:
                candidates.append(candidate2)
                if qs:
                    candidates.append(candidate2 + qs)
            for candidate in candidates:
                if cancellation_event.is_set():
                    return "CANCELLED"
                self._log(f"Trying: {candidate}")
                async with semaphore:
                    if cancellation_event.is_set():
                        return "CANCELLED"
                    try:
                        async with self.session.get(candidate, allow_redirects=False) as response:
                            if response.status == 200:
                                if response.url and str(response.url).rstrip("/") != candidate.rstrip("/"):
                                    self._log(f"Detected redirect to error page: {response.url}")
                                    return "STOP_SERIES"
                                filename = safe_filename(os.path.basename(candidate))
                                filepath = os.path.join(folder, filename)
                                if os.path.exists(filepath):
                                    timestamp = time.strftime("%Y%m%d%H%M%S")
                                    filename = f"{timestamp}-{filename}"
                                    filepath = os.path.join(folder, filename)
                                total_downloaded = 0
                                start_time = time.time()
                                with open(filepath, "wb") as f:
                                    while True:
                                        if cancellation_event.is_set():
                                            return "CANCELLED"
                                        chunk = await response.content.read(1024)
                                        if not chunk:
                                            break
                                        f.write(chunk)
                                        total_downloaded += len(chunk)
                                        elapsed = time.time() - start_time
                                        expected_time = total_downloaded / speed_limit
                                        if expected_time > elapsed:
                                            sleep_interval = min(expected_time - elapsed, 0.1)
                                            await asyncio.sleep(sleep_interval)
                                        if self.progress_callback:
                                            self.progress_callback(candidate, total_downloaded)
                                self._log(f"Downloaded: {filename}")
                                if self.gallery_callback:
                                    self.gallery_callback(filepath)
                                return candidate
                            else:
                                self._log(f"HTTP {response.status} for {candidate}")
                    except Exception as e:
                        self._log(f"Exception downloading {candidate}: {e}")
            return None

        start_result = await try_download(start_index)
        if start_result in (None, "STOP_SERIES", "CANCELLED"):
            self._log("ERROR: Could not download the starting file. Aborting series.")
            return

        async def download_direction(start, step):
            i = start
            while True:
                if cancellation_event.is_set():
                    self._log("Download series canceled (direction loop).")
                    break
                result = await try_download(i)
                if result is None:
                    self._log("No more images found in this direction.")
                    break
                if result in ("STOP_SERIES", "CANCELLED"):
                    self._log("Stopping downloads due to cancellation or error page detection.")
                    break
                i += step

        await asyncio.gather(
            download_direction(start_index + 1, 1),
            download_direction(start_index - 1, -1)
        )
        self._log("Download series complete.")

    def _log(self, message):
        if self.log_callback:
            self.log_callback(message)
        else:
            logging.info(message)

#############################
# PARSER MODULE (Async)
#############################
class Parser:
    def __init__(self, log_callback=None, progress_callback=None, gallery_callback=None):
        self.log_callback = log_callback
        self.progress_callback = progress_callback
        self.gallery_callback = gallery_callback
        self.session = None

    async def _create_session(self):
        headers = {'User-Agent': 'MyDownloader/1.0 (https://example.com)'}
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)

    async def close(self):
        if self.session:
            await self.session.close()

    async def parse_page(self, page_url, folder, min_size_kb, min_short_side):
        if not self.session:
            await self._create_session()
        self._log(f"Parsing page: {page_url}")
        try:
            async with self.session.get(page_url) as r:
                text = await r.text()
        except Exception as e:
            self._log(f"Error loading webpage: {e}")
            return
        soup = BeautifulSoup(text, "html.parser")
        candidates = set()
        for img in soup.find_all("img"):
            src = img.get("src")
            if src:
                candidates.add(urljoin(page_url, src))
        count_saved = 0
        tasks = []
        for img_url in candidates:
            tasks.append(self.process_image(img_url, folder, min_size_kb, min_short_side))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if res:
                count_saved += 1
        self._log(f"Parse complete. {count_saved} images saved from page.")

    async def process_image(self, img_url, folder, min_size_kb, min_short_side):
        try:
            async with self.session.head(img_url) as head:
                if "Content-Length" in head.headers:
                    size = int(head.headers["Content-Length"])
                    if size < min_size_kb * 1024:
                        return None
                else:
                    size = None
        except Exception:
            return None
        try:
            async with self.session.get(img_url) as r_img:
                if r_img.status != 200:
                    return None
                data = await r_img.read()
        except Exception:
            return None
        if size is None:
            size = len(data)
            if size < min_size_kb * 1024:
                return None
        try:
            from io import BytesIO
            im = Image.open(BytesIO(data))
            width, height = im.size
            if min(width, height) < min_short_side:
                return None
        except Exception:
            return None
        filename = safe_filename(os.path.basename(img_url.split("?")[0]))
        if not filename:
            filename = f"image_{int(time.time())}.jpg"
        filepath = os.path.join(folder, filename)
        if os.path.exists(filepath):
            timestamp = time.strftime("%Y%m%d%H%M%S")
            filename = f"{timestamp}-{filename}"
            filepath = os.path.join(folder, filename)
        try:
            with open(filepath, "wb") as f:
                f.write(data)
            self._log(f"Saved image: {filename}")
            if self.gallery_callback:
                self.gallery_callback(filepath)
            return True
        except Exception as e:
            self._log(f"Error saving image {filename}: {e}")
            return None

    def _log(self, message):
        if self.log_callback:
            self.log_callback(message)
        else:
            logging.info(message)

#############################
# UI MODULE
#############################
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Series Downloader")
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self._setup_logging()

        # Create an asyncio event loop in its own thread.
        self.loop = asyncio.new_event_loop()
        self.async_thread = threading.Thread(target=self.start_loop, daemon=True)
        self.async_thread.start()

        # Downloader and Parser instances (created on demand)
        self.downloader = None
        self.parser = None

        # Cancellation event for downloads.
        self.download_cancel_event = threading.Event()

        # For duplicate management (for undo deletion)
        self.deleted_files_backup = {}  # original file path -> backup path
        self.duplicate_groups = []      # each element is a dict: {"files": [...], "keeper_var": StringVar()}

        # Create a BooleanVar for the "Include Subfolders" checkbox.
        self.include_subfolders_var = tk.BooleanVar(value=False)

        self.create_widgets()

    def _setup_logging(self):
        self.logger = logging.getLogger("DownloaderApp")
        self.logger.setLevel(logging.DEBUG)
        handler = TextHandler(self)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def create_widgets(self):
        # --- Top Frame: contains Input Controls (left column) and Action Controls (right column) ---
        top_frame = tk.Frame(self)
        top_frame.pack(padx=5, pady=5, fill=tk.X)

        # Left Column: Input Controls
        left_frame = tk.Frame(top_frame)
        left_frame.grid(row=0, column=0, sticky="w")
        # URL Input
        url_frame = tk.Frame(left_frame)
        url_frame.pack(fill=tk.X, pady=2)
        tk.Label(url_frame, text="Enter URL:").pack(side=tk.LEFT)
        self.url_entry = tk.Entry(url_frame, width=70)
        self.url_entry.pack(side=tk.LEFT, padx=(5, 0))
        self.url_entry.bind("<Button-3>", self.show_context_menu)
        tk.Button(url_frame, text="Clear", command=lambda: self.url_entry.delete(0, tk.END)).pack(side=tk.LEFT, padx=5)
        # Target Folder Input
        folder_frame = tk.Frame(left_frame)
        folder_frame.pack(fill=tk.X, pady=2)
        tk.Label(folder_frame, text="Target Folder:").pack(side=tk.LEFT)
        self.folder_path = tk.StringVar()
        self.folder_entry = tk.Entry(folder_frame, width=70, textvariable=self.folder_path)
        self.folder_entry.pack(side=tk.LEFT, padx=(5, 0))
        tk.Button(folder_frame, text="Browse", command=self.browse_folder).pack(side=tk.LEFT, padx=5)
        # Download Settings and Parser Options
        settings_frame = tk.Frame(left_frame)
        settings_frame.pack(fill=tk.X, pady=2)
        tk.Label(settings_frame, text="Max simultaneous downloads:").pack(side=tk.LEFT)
        self.concurrent_spinbox = tk.Spinbox(settings_frame, from_=1, to=5, width=5)
        self.concurrent_spinbox.pack(side=tk.LEFT, padx=(5, 20))
        tk.Label(settings_frame, text="Download speed limit (kB/s):").pack(side=tk.LEFT)
        self.speed_entry = tk.Entry(settings_frame, width=10)
        self.speed_entry.pack(side=tk.LEFT, padx=5)
        self.speed_entry.insert(0, "1024")
        tk.Label(settings_frame, text="Min File Size (kB):").pack(side=tk.LEFT, padx=(20, 0))
        self.min_size_var = tk.StringVar(value="64")
        tk.OptionMenu(settings_frame, self.min_size_var, "32", "64", "128", "256", "512", "1024").pack(side=tk.LEFT, padx=5)
        tk.Label(settings_frame, text="Min Short Side (pix):").pack(side=tk.LEFT)
        self.min_short_side_entry = tk.Entry(settings_frame, width=5)
        self.min_short_side_entry.insert(0, "257")
        self.min_short_side_entry.pack(side=tk.LEFT, padx=5)

        # Right Column: Action & Duplicate Check Controls (arranged side-by-side)
        right_frame = tk.Frame(top_frame)
        right_frame.grid(row=0, column=1, sticky="ne", padx=10)

        # Action Buttons Frame (Left in right_frame)
        action_frame = tk.Frame(right_frame, bd=2, relief=tk.GROOVE)
        action_frame.grid(row=0, column=0, padx=5, pady=2, sticky="nw")
        self.go_button = tk.Button(action_frame, text="Go", command=self.go_pressed, width=10)
        self.go_button.grid(row=0, column=0, padx=5, pady=2, sticky="w")
        self.stop_button = tk.Button(action_frame, text="Stop/Cancel", command=self.stop_cancel, state=tk.DISABLED, width=10)
        self.stop_button.grid(row=1, column=0, padx=5, pady=2, sticky="w")
        self.pause_button = tk.Button(action_frame, text="Pause", command=self.pause_download, state=tk.DISABLED, width=10)
        self.pause_button.grid(row=2, column=0, padx=5, pady=2, sticky="w")
        self.resume_button = tk.Button(action_frame, text="Resume", command=self.resume_download, state=tk.DISABLED, width=10)
        self.resume_button.grid(row=3, column=0, padx=5, pady=2, sticky="w")
        # New: Restore Image button (calls undo_delete)
        self.restore_button = tk.Button(action_frame, text="Restore Image", command=self.undo_delete, width=10)
        self.restore_button.grid(row=4, column=0, padx=5, pady=2, sticky="w")

        # Duplicate Check Controls Frame (Right in right_frame)
        dup_frame = tk.Frame(right_frame, bd=2, relief=tk.GROOVE)
        dup_frame.grid(row=0, column=1, padx=5, pady=2, sticky="ne")
        # Configure the column so that the buttons expand and center.
        dup_frame.columnconfigure(0, weight=1)
        self.dup_check_button = tk.Button(dup_frame, text="Duplicate Check", command=self.check_duplicates, width=12)
        self.dup_check_button.grid(row=0, column=0, padx=5, pady=2, sticky="ew")
        self.include_subfolders_check = tk.Checkbutton(dup_frame, text="Include Subfolders", variable=self.include_subfolders_var)
        self.include_subfolders_check.grid(row=1, column=0, padx=5, pady=2, sticky="ew")
        self.remove_dup_button = tk.Button(dup_frame, text="Remove Duplicates", command=self.remove_duplicates_callback, width=12)
        self.remove_dup_button.grid(row=2, column=0, padx=5, pady=2, sticky="ew")
        self.keep_newest_button = tk.Button(dup_frame, text="Keep Newest", command=self.keep_newest, width=12)
        self.keep_newest_button.grid(row=3, column=0, padx=5, pady=2, sticky="ew")
        self.keep_oldest_button = tk.Button(dup_frame, text="Keep Oldest", command=self.keep_oldest, width=12)
        self.keep_oldest_button.grid(row=4, column=0, padx=5, pady=2, sticky="ew")

        # --- Progress Bar ---
        self.progress_bar = ttk.Progressbar(self, orient="horizontal", mode="determinate")
        self.progress_bar.pack(fill=tk.X, padx=5, pady=5)
        self.progress_bar['value'] = 0

        # --- Notebook for Job Queue and Duplicate Results ---
        self.display_notebook = ttk.Notebook(self)
        self.display_notebook.pack(padx=5, pady=5, fill=tk.X)
        # Job Queue Tab (fixed height)
        self.job_queue_tab = tk.Frame(self.display_notebook, height=150)
        self.job_queue_tab.pack_propagate(False)
        self.display_notebook.add(self.job_queue_tab, text="Job Queue")
        self.job_queue_canvas = tk.Canvas(self.job_queue_tab, height=150, borderwidth=0, highlightthickness=0)
        self.job_queue_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.job_list_scrollbar = tk.Scrollbar(self.job_queue_tab, orient=tk.VERTICAL, command=self.job_queue_canvas.yview)
        self.job_list_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.job_queue_canvas.configure(yscrollcommand=self.job_list_scrollbar.set)
        self.job_list_frame = tk.Frame(self.job_queue_canvas)
        self.job_queue_canvas.create_window((0, 0), window=self.job_list_frame, anchor="nw")
        self.job_list_frame.bind("<Configure>", lambda e: self.job_queue_canvas.configure(scrollregion=self.job_queue_canvas.bbox("all")))

        # Duplicate Results Tab (fixed height)
        self.dup_results_tab = tk.Frame(self.display_notebook, height=150)
        self.dup_results_tab.pack_propagate(False)
        self.display_notebook.add(self.dup_results_tab, text="Duplicate Results")
        self.dup_canvas = tk.Canvas(self.dup_results_tab, height=150, borderwidth=0, highlightthickness=0, bg="#ffeeee")
        self.dup_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.dup_scrollbar = tk.Scrollbar(self.dup_results_tab, orient=tk.VERTICAL, command=self.dup_canvas.yview)
        self.dup_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.dup_canvas.configure(yscrollcommand=self.dup_scrollbar.set)
        self.dup_frame = tk.Frame(self.dup_canvas, bg="#ffeeee")
        self.dup_canvas.create_window((0, 0), window=self.dup_frame, anchor="nw")
        self.dup_frame.bind("<Configure>", lambda e: self.dup_canvas.configure(scrollregion=self.dup_canvas.bbox("all")))

        # --- Log Area ---
        self.log_text = tk.Text(self, height=15, state=tk.DISABLED)
        self.log_text.pack(pady=5, padx=5, fill=tk.BOTH, expand=True)

        # --- Gallery ---
        tk.Label(self, text="Downloaded Images Gallery:").pack(pady=5)
        self.gallery_canvas = tk.Canvas(self, height=120)
        self.gallery_canvas.pack(fill=tk.X, padx=5)
        self.gallery_scrollbar = tk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.gallery_canvas.xview)
        self.gallery_scrollbar.pack(fill=tk.X, padx=5)
        self.gallery_canvas.configure(xscrollcommand=self.gallery_scrollbar.set)
        self.gallery_frame = tk.Frame(self.gallery_canvas)
        self.gallery_canvas.create_window((0, 0), window=self.gallery_frame, anchor="nw")
        self.gallery_frame.bind("<Configure>", lambda e: self.gallery_canvas.configure(scrollregion=self.gallery_canvas.bbox("all")))
        self.gallery_images = []

    #############################
    # UI HELPER METHODS
    #############################
    def show_context_menu(self, event):
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="Cut", command=lambda: event.widget.event_generate("<<Cut>>"))
        menu.add_command(label="Copy", command=lambda: event.widget.event_generate("<<Copy>>"))
        menu.add_command(label="Paste", command=lambda: event.widget.event_generate("<<Paste>>"))
        menu.tk_popup(event.x_root, event.y_root)

    def browse_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.folder_path.set(folder)

    def log(self, message):
        self.after(0, self._append_log, message)

    def _append_log(self, message):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)

    def update_progress(self, url, downloaded_bytes):
        self.after(0, lambda: self.progress_bar.step(1))

    def add_gallery_image(self, filepath):
        self.after(0, self._add_gallery_image, filepath)

    def _add_gallery_image(self, filepath):
        frame = tk.Frame(self.gallery_frame, bd=1, relief=tk.RIDGE)
        frame.pack(side=tk.RIGHT, padx=5, pady=5)
        try:
            img = Image.open(filepath)
            thumbnail_height = 100
            aspect = img.width / img.height
            thumbnail_width = int(thumbnail_height * aspect)
            img.thumbnail((thumbnail_width, thumbnail_height))
            photo = ImageTk.PhotoImage(img)
        except Exception as e:
            self.log(f"Error loading image {filepath}: {e}")
            return
        label = tk.Label(frame, image=photo)
        label.image = photo
        label.pack()
        label.bind("<Double-Button-1>", lambda e, path=filepath: self.open_image(path))
        del_btn = tk.Button(frame, text="X", command=lambda f=frame, path=filepath: self.delete_image(f, path),
                            relief=tk.FLAT, bd=0, highlightthickness=0, fg="red",
                            bg=frame.cget("bg"), activebackground=frame.cget("bg"))
        del_btn.place(relx=1, rely=0, anchor="ne")
        self.gallery_images.insert(0, photo)
        if len(self.gallery_frame.winfo_children()) > 30:
            oldest_frame = self.gallery_frame.winfo_children()[0]
            oldest_frame.destroy()
        if len(self.gallery_images) > 30:
            self.gallery_images.pop(-1)

    def open_image(self, filepath):
        system = platform.system()
        try:
            if system == "Windows":
                os.startfile(filepath)
            elif system == "Darwin":
                subprocess.Popen(["open", filepath])
            else:
                subprocess.Popen(["xdg-open", filepath])
        except Exception as e:
            self.log(f"Error opening image: {e}")

    def delete_image(self, frame, filepath):
        backup_folder = os.path.join(os.path.dirname(filepath), "deleted_backup")
        os.makedirs(backup_folder, exist_ok=True)
        backup_path = os.path.join(backup_folder, os.path.basename(filepath))
        try:
            shutil.move(filepath, backup_path)
            self.deleted_files_backup[filepath] = backup_path
            self.log(f"Deleted image: {os.path.basename(filepath)} (can be undone)")
        except Exception as e:
            self.log(f"Error deleting image: {e}")
        frame.destroy()

    def undo_delete(self):
        restored = 0
        for original, backup in list(self.deleted_files_backup.items()):
            try:
                shutil.move(backup, original)
                restored += 1
                del self.deleted_files_backup[original]
                self.log(f"Restored: {original}")
            except Exception as e:
                self.log(f"Error restoring {original}: {e}")
        if restored == 0:
            self.log("No files to restore.")

    #############################
    # DUPLICATE CHECK & RESULTS DISPLAY
    #############################
    def check_duplicates(self):
        folder = self.folder_path.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showerror("Input Error", "Please select a valid target folder.")
            return
        threading.Thread(target=self.perform_duplicate_check, args=(folder, self.include_subfolders_var.get()), daemon=True).start()

    def perform_duplicate_check(self, folder, include_subfolders):
        self.log("Starting duplicate check...")
        file_dict = {}
        if include_subfolders:
            for root, dirs, files in os.walk(folder):
                for name in files:
                    filepath = os.path.join(root, name)
                    try:
                        size = os.path.getsize(filepath)
                    except Exception:
                        continue
                    file_dict.setdefault(size, []).append(filepath)
        else:
            for name in os.listdir(folder):
                filepath = os.path.join(folder, name)
                if os.path.isfile(filepath):
                    try:
                        size = os.path.getsize(filepath)
                    except Exception:
                        continue
                    file_dict.setdefault(size, []).append(filepath)
        dup_groups = []
        for size, files in file_dict.items():
            if len(files) < 2:
                continue
            hash_dict = {}
            for filepath in files:
                h = compute_md5(filepath)
                if h is None:
                    continue
                hash_dict.setdefault(h, []).append(filepath)
            for h, file_list in hash_dict.items():
                if len(file_list) > 1:
                    dup_groups.append(file_list)
        self.log(f"Duplicate check complete. Found {len(dup_groups)} duplicate groups.")
        # Bring Duplicate Results tab to foreground.
        self.after(0, lambda: self.display_notebook.select(self.dup_results_tab))
        self.display_duplicates(dup_groups)

    def display_duplicates(self, dup_groups):
        # Clear previous duplicate display.
        for widget in self.dup_frame.winfo_children():
            widget.destroy()
        self.duplicate_groups = []
        for group in dup_groups:
            sorted_group = sorted(group, key=lambda f: (len(os.path.dirname(f)), len(os.path.basename(f)), os.path.basename(f)))
            keeper_var = tk.StringVar(value=sorted_group[0])
            group_dict = {"files": sorted_group, "keeper_var": keeper_var}
            self.duplicate_groups.append(group_dict)
            group_frame = tk.Frame(self.dup_frame, bg="#ffeeee", pady=2)
            group_frame.pack(fill=tk.X, padx=5, pady=2)
            tk.Label(group_frame, text="Duplicate Group:", bg="#ffeeee").pack(anchor="w")
            for file in sorted_group:
                row_frame = tk.Frame(group_frame, bg="#ffeeee")
                row_frame.pack(fill=tk.X, padx=10, pady=1)
                # Center the radiobutton inside the row_frame.
                rb = tk.Radiobutton(row_frame, text=file, variable=keeper_var, value=file, bg="#ffeeee", anchor="center")
                rb.pack(side=tk.LEFT, fill=tk.X, expand=True)
                preview_btn = tk.Button(row_frame, text="Preview", command=lambda f=file: self.open_image(f))
                preview_btn.pack(side=tk.RIGHT)
            sep = tk.Frame(self.dup_frame, height=2, bd=1, relief=tk.SUNKEN, bg="#ffeeee")
            sep.pack(fill=tk.X, padx=5, pady=2)

    def remove_duplicates(self, group):
        keeper = group["keeper_var"].get()
        total_space_recovered = 0
        for f in group["files"]:
            if f != keeper:
                try:
                    size = os.path.getsize(f)
                    total_space_recovered += size
                    backup_folder = os.path.join(os.path.dirname(f), "deleted_backup")
                    os.makedirs(backup_folder, exist_ok=True)
                    backup_path = os.path.join(backup_folder, os.path.basename(f))
                    shutil.move(f, backup_path)
                    self.deleted_files_backup[f] = backup_path
                    self.log(f"Deleted duplicate: {f}")
                except Exception as e:
                    self.log(f"Error deleting {f}: {e}")
        recovered_mb = total_space_recovered / (1024*1024)
        self.log(f"Duplicate cleaning complete. Total space recovered: {recovered_mb:.2f} MB")
        self.duplicate_groups.remove(group)
        remaining = [grp["files"] for grp in self.duplicate_groups]
        self.display_duplicates(remaining)

    def remove_duplicates_callback(self):
        if not self.duplicate_groups:
            self.log("No duplicate groups to remove.")
            return
        total_space_recovered = 0
        for group in self.duplicate_groups:
            keeper = group["keeper_var"].get()
            for f in group["files"]:
                if f != keeper:
                    try:
                        size = os.path.getsize(f)
                        total_space_recovered += size
                        backup_folder = os.path.join(os.path.dirname(f), "deleted_backup")
                        os.makedirs(backup_folder, exist_ok=True)
                        backup_path = os.path.join(backup_folder, os.path.basename(f))
                        shutil.move(f, backup_path)
                        self.deleted_files_backup[f] = backup_path
                        self.log(f"Deleted duplicate: {f}")
                    except Exception as e:
                        self.log(f"Error deleting {f}: {e}")
        recovered_mb = total_space_recovered / (1024*1024)
        self.log(f"Duplicate cleaning complete. Total space recovered: {recovered_mb:.2f} MB")
        self.duplicate_groups = []
        self.display_duplicates([])

    def keep_newest(self):
        # For each duplicate group, set keeper_var to the file with the highest modification time.
        for group in self.duplicate_groups:
            newest = max(group["files"], key=lambda f: os.path.getmtime(f) if os.path.exists(f) else 0)
            group["keeper_var"].set(newest)
        self.log("Selected newest file in each duplicate group.")

    def keep_oldest(self):
        # For each duplicate group, set keeper_var to the file with the lowest modification time.
        for group in self.duplicate_groups:
            oldest = min(group["files"], key=lambda f: os.path.getmtime(f) if os.path.exists(f) else float('inf'))
            group["keeper_var"].set(oldest)
        self.log("Selected oldest file in each duplicate group.")

    #############################
    # ACTIVE JOB CONTROL & SHUTDOWN
    #############################
    def go_pressed(self):
        url = self.url_entry.get().strip()
        folder = self.folder_path.get().strip()
        valid = True
        if not url:
            self.url_entry.config(bg="pink")
            valid = False
        else:
            self.url_entry.config(bg="white")
        if not folder or not os.path.isdir(folder):
            self.folder_entry.config(bg="pink")
            valid = False
        else:
            self.folder_entry.config(bg="white")
        if not valid:
            self.log("Please correct the highlighted fields.")
            return

        # Bring Job Queue tab to front.
        self.display_notebook.select(self.job_queue_tab)

        if url.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
            try:
                max_simultaneous = int(self.concurrent_spinbox.get())
            except ValueError:
                self.log("Invalid value for simultaneous downloads.")
                return
            try:
                speed_kbs = int(self.speed_entry.get())
                if speed_kbs < 25 or speed_kbs > 51200:
                    raise ValueError
            except ValueError:
                self.log("Download speed must be between 25 and 51200 KB/s.")
                return
            speed_limit = speed_kbs * 1024
            self.downloader = Downloader(log_callback=self.log, progress_callback=self.update_progress, gallery_callback=self.add_gallery_image)
            self.download_cancel_event.clear()
            self.stop_button.config(state=tk.NORMAL)
            self.pause_button.config(state=tk.NORMAL)
            self.go_button.config(state=tk.DISABLED)
            asyncio.run_coroutine_threadsafe(
                self.downloader.download_series(url, folder, max_simultaneous, speed_limit, self.download_cancel_event),
                self.loop
            ).add_done_callback(lambda fut: self.after(0, self.download_complete))
        else:
            try:
                min_size_kb = int(self.min_size_var.get())
            except ValueError:
                min_size_kb = 64
            try:
                min_short_side = int(self.min_short_side_entry.get())
            except ValueError:
                min_short_side = 257
            self.parser = Parser(log_callback=self.log, gallery_callback=self.add_gallery_image)
            self.go_button.config(state=tk.DISABLED)
            asyncio.run_coroutine_threadsafe(
                self.parser.parse_page(url, folder, min_size_kb, min_short_side),
                self.loop
            ).add_done_callback(lambda fut: self.after(0, self.download_complete))

    def download_complete(self):
        self.log("Job complete.")
        self.go_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.pause_button.config(state=tk.DISABLED)
        self.resume_button.config(state=tk.DISABLED)
        self.progress_bar['value'] = 0

    def stop_cancel(self):
        self.log("Stop/Cancel pressed: Cancelling operations.")
        if self.downloader:
            self.download_cancel_event.set()
        self.go_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.pause_button.config(state=tk.DISABLED)
        self.resume_button.config(state=tk.DISABLED)

    def pause_download(self):
        self.log("Pause pressed. (Pause functionality is not fully implemented in async mode)")
        self.pause_button.config(state=tk.DISABLED)
        self.resume_button.config(state=tk.NORMAL)

    def resume_download(self):
        self.log("Resume pressed.")
        self.pause_button.config(state=tk.NORMAL)
        self.resume_button.config(state=tk.DISABLED)

    def on_close(self):
        self.log("Shutting down...")
        if self.downloader:
            self.download_cancel_event.set()
            asyncio.run_coroutine_threadsafe(self.downloader.close(), self.loop)
        if self.parser:
            asyncio.run_coroutine_threadsafe(self.parser.close(), self.loop)
        # Remove the deleted_backup folder from the target folder (if it exists)
        folder = self.folder_path.get().strip()
        if folder and os.path.isdir(folder):
            backup_folder = os.path.join(folder, "deleted_backup")
            if os.path.exists(backup_folder):
                try:
                    shutil.rmtree(backup_folder)
                    self.log("Removed deleted_backup folder.")
                except Exception as e:
                    self.log(f"Error removing deleted_backup folder: {e}")
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.destroy()

#############################
# Logging Handler to update Tkinter log widget
#############################
class TextHandler(logging.Handler):
    def __init__(self, app):
        super().__init__()
        self.app = app

    def emit(self, record):
        msg = self.format(record)
        self.app.after(0, self.app._append_log, msg)

#############################
# MAIN
#############################
if __name__ == "__main__":
    app = App()
    app.mainloop()
