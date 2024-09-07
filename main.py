import os
import shutil
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import zipfile

def process_files(input_folder1, input_folder2):
    # Create input folders if they don't exist
    os.makedirs(input_folder1, exist_ok=True)
    os.makedirs(input_folder2, exist_ok=True)

    def unzip_file(zip_file_path, extract_folder):
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)

    # Unzip any ZIP files in input_folder1
    zip_files = [f for f in os.listdir(input_folder1) if f.endswith('.zip')]
    for zip_file in zip_files:
        zip_path = os.path.join(input_folder1, zip_file)
        unzip_file(zip_path, input_folder1)
        os.remove(zip_path)

    # Function to move the newest file from input_folder1 to input_folder2
    def move_newest_file():
        files = os.listdir(input_folder1)
        if not files:
            print(f"No files found in {input_folder1}.")
            return

        # Define keywords to categorize files
        keywords = ["BBH", "npo", "mapping_updated", "Cell ID and Cell name"]

        # Create dictionaries to hold files for each keyword
        keyword_files = {keyword: [] for keyword in keywords}

        # Categorize files based on keywords
        for keyword in keywords:
            if keyword == "Cell ID and Cell name":
                keyword_files[keyword] = [f for f in files if "Cell ID" in f or "Cell name" in f]
            else:
                keyword_files[keyword] = [f for f in files if keyword in f]

        # Move and delete files for each keyword
        for keyword, files_list in keyword_files.items():
            move_and_delete(files_list, keyword)

    def move_and_delete(files_list, keyword):
        if not files_list:
            return

        newest_file = max(files_list, key=lambda f: os.path.getmtime(os.path.join(input_folder1, f)))

        src_path = os.path.join(input_folder1, newest_file)
        dest_path = os.path.join(input_folder2, newest_file)

        # Move the newest file to input_folder2
        shutil.move(src_path, dest_path)
        print(f"Moved {newest_file} from {input_folder1} to {input_folder2}.")

        # Delete the oldest file with the same keyword in input_folder2
        files_in_dest = os.listdir(input_folder2)
        matching_files = [f for f in files_in_dest if keyword in f]

        if len(matching_files) > 1:
            oldest_file = min(matching_files, key=lambda f: os.path.getmtime(os.path.join(input_folder2, f)))
            file_to_delete = os.path.join(input_folder2, oldest_file)
            os.remove(file_to_delete)
            print(f"Deleted the oldest {keyword} file {oldest_file} from {input_folder2}.")

    # Function to handle CSV files
    def handle_csv_file(file_path):
        df = pd.read_csv(file_path)
        print(f"Processing CSV file: {file_path}")
        # Add your CSV processing logic here

    # Function to handle XLSX files
    def handle_xlsx_file(file_path):
        df = pd.read_excel(file_path)
        print(f"Processing XLSX file: {file_path}")
        # Add your XLSX processing logic here

    # Initial move of existing files
    move_newest_file()

    class FileEventHandler(FileSystemEventHandler):
        def on_created(self, event):
            if event.is_directory:
                return
            file_path = event.src_path
            if file_path.endswith('.csv') or file_path.endswith('.xlsx'):
                move_newest_file()
                if file_path.endswith('.csv'):
                    handle_csv_file(file_path)
                elif file_path.endswith('.xlsx'):
                    handle_xlsx_file(os.path.join(input_folder2, os.path.basename(file_path)))

    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path=input_folder1, recursive=False)
    observer.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    input_folder1 = 'input_folder1'
    input_folder2 = 'input_folder2'
    process_files(input_folder1, input_folder2)