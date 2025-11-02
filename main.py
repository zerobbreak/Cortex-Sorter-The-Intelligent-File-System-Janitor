import sys
import time
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
import sqlite3
import hashlib
import yaml
import pdfplumber
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

# --- Configuration & Setup ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

CONFIG_FILE = "rules.yml"
DB_NAME = "file_hashes.db"
HASH_ALGORITHM = hashlib.sha256
CHUNK_SIZE = 8192  # Increased from 4096 for better performance
FILE_STABILITY_DELAY = 1.5  # Wait for file to finish writing


# --- Data Classes for Type Safety ---

@dataclass
class SortingRule:
    """Represents a file sorting rule with all matching criteria."""
    name: str
    dest: Path
    extensions: Optional[List[str]] = None
    name_contains: Optional[List[str]] = None
    content_contains: Optional[List[str]] = None
    rule_type: Optional[str] = None

    def __post_init__(self):
        """Normalize extensions to lowercase with leading dots."""
        if self.extensions:
            self.extensions = [
                ext.lower() if ext.startswith('.') else f'.{ext.lower()}'
                for ext in self.extensions
            ]
        # Normalize keywords to lowercase for case-insensitive matching
        if self.name_contains:
            self.name_contains = [kw.lower() for kw in self.name_contains]
        if self.content_contains:
            self.content_contains = [kw.lower() for kw in self.content_contains]


@dataclass
class Config:
    """Application configuration."""
    source_folder: Path
    duplicate_folder: Path
    rules: List[SortingRule]


# --- Database Module with Connection Pooling ---

class DatabaseManager:
    """Manages database operations with improved error handling."""
    
    def __init__(self, db_path: str = DB_NAME):
        self.db_path = db_path
        self._init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_database(self):
        """Creates the database schema if it doesn't exist."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_hashes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sha256_hash TEXT UNIQUE NOT NULL,
                    original_path TEXT NOT NULL,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    file_size INTEGER,
                    file_name TEXT
                );
            """)
            # Create index for faster lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_sha256_hash 
                ON file_hashes(sha256_hash);
            """)
        logger.info(f"Database '{self.db_path}' initialized successfully")
    
    def get_file_hash(self, file_path: Path) -> Optional[str]:
        """
        Calculates SHA-256 hash with improved error handling and performance.
        Returns None if file cannot be hashed.
        """
        hash_obj = HASH_ALGORITHM()
        try:
            file_size = file_path.stat().st_size
            with open(file_path, 'rb') as f:
                while chunk := f.read(CHUNK_SIZE):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except (IOError, OSError, PermissionError) as e:
            logger.warning(f"Cannot hash '{file_path.name}': {e}")
            return None
    
    def is_duplicate(self, file_hash: str) -> bool:
        """Checks if hash exists in database."""
        if not file_hash:
            return False
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM file_hashes WHERE sha256_hash = ? LIMIT 1",
                (file_hash,)
            )
            return cursor.fetchone() is not None
    
    def add_file_record(self, file_hash: str, file_path: Path) -> bool:
        """
        Adds file record to database with additional metadata.
        Returns True if added, False if already exists.
        """
        if not file_hash:
            return False
        
        try:
            file_size = file_path.stat().st_size
        except (OSError, FileNotFoundError):
            file_size = None
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO file_hashes 
                    (sha256_hash, original_path, file_size, file_name) 
                    VALUES (?, ?, ?, ?)
                """, (file_hash, str(file_path), file_size, file_path.name))
                return True
            except sqlite3.IntegrityError:
                logger.debug(f"Hash {file_hash[:12]}... already exists")
                return False
    
    def get_duplicate_info(self, file_hash: str) -> Optional[Tuple[str, str]]:
        """Returns original path and first seen date for a duplicate."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT original_path, first_seen 
                FROM file_hashes 
                WHERE sha256_hash = ?
            """, (file_hash,))
            result = cursor.fetchone()
            return result if result else None


# --- PDF Content Analysis Module ---

class PDFAnalyzer:
    """Handles PDF content extraction and keyword matching."""
    
    @staticmethod
    def extract_text(file_path: Path, max_pages: Optional[int] = None) -> Optional[str]:
        """
        Extracts text from PDF with pagination support.
        Returns None if extraction fails.
        """
        try:
            content_parts = []
            with pdfplumber.open(file_path) as pdf:
                pages_to_process = pdf.pages[:max_pages] if max_pages else pdf.pages
                
                for page in pages_to_process:
                    text = page.extract_text()
                    if text:
                        content_parts.append(text)
            
            return ' '.join(content_parts) if content_parts else None
            
        except Exception as e:
            logger.warning(f"PDF extraction failed for '{file_path.name}': {e}")
            return None
    
    @staticmethod
    def contains_keywords(content: str, keywords: List[str]) -> Tuple[bool, Optional[str]]:
        """
        Checks if content contains any of the keywords.
        Returns (match_found, matched_keyword).
        """
        if not content or not keywords:
            return False, None
        
        content_lower = content.lower()
        for keyword in keywords:
            if keyword.lower() in content_lower:
                return True, keyword
        
        return False, None


# --- File System Operations ---

class FileOperations:
    """Handles file system operations with conflict resolution."""
    
    @staticmethod
    def get_unique_path(destination_path: Path, max_attempts: int = 1000) -> Path:
        """
        Generates unique path by appending counter if conflicts exist.
        Raises RuntimeError if max_attempts reached.
        """
        if not destination_path.exists():
            return destination_path
        
        for counter in range(1, max_attempts):
            new_path = destination_path.with_name(
                f"{destination_path.stem}_{counter}{destination_path.suffix}"
            )
            if not new_path.exists():
                return new_path
        
        raise RuntimeError(f"Could not generate unique path after {max_attempts} attempts")
    
    @staticmethod
    def wait_for_file_stability(file_path: Path, delay: float = FILE_STABILITY_DELAY) -> bool:
        """
        Waits for file to finish writing by checking size stability.
        Returns True if file is stable, False if it disappeared.
        """
        time.sleep(delay)
        
        if not file_path.exists():
            return False
        
        try:
            # Check if we can open the file exclusively
            with open(file_path, 'rb') as f:
                pass
            return True
        except (IOError, PermissionError):
            # File is still being written
            time.sleep(delay)
            return file_path.exists()
    
    @staticmethod
    def safe_move(source: Path, destination: Path) -> bool:
        """
        Safely moves file with error handling.
        Returns True on success, False on failure.
        """
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            source.rename(destination)
            return True
        except (OSError, PermissionError) as e:
            logger.error(f"Failed to move '{source.name}' to '{destination}': {e}")
            return False


# --- Core Sorting Engine ---

class FileSorter:
    """Main file sorting logic with rule matching."""
    
    def __init__(self, config: Config, db_manager: DatabaseManager):
        self.config = config
        self.db = db_manager
        self.pdf_analyzer = PDFAnalyzer()
        self.file_ops = FileOperations()
    
    def should_ignore_file(self, file_path: Path) -> bool:
        """Determines if file should be ignored."""
        name = file_path.name
        # Ignore hidden files, temp files, and system files
        ignored_patterns = ('.', '~', 'Thumbs.db', '.DS_Store')
        ignored_extensions = ('.tmp', '.crdownload', '.part')
        
        return (
            any(name.startswith(p) for p in ignored_patterns) or
            file_path.suffix.lower() in ignored_extensions
        )
    
    def handle_duplicate(self, file_path: Path, file_hash: str) -> bool:
        """
        Moves duplicate file to duplicate folder.
        Returns True if successfully handled.
        """
        dup_info = self.db.get_duplicate_info(file_hash)
        original_path = dup_info[0] if dup_info else "unknown"
        
        logger.info(f"Duplicate detected: '{file_path.name}' (original: {Path(original_path).name})")
        
        dest_path = self.file_ops.get_unique_path(
            self.config.duplicate_folder / file_path.name
        )
        
        if self.file_ops.safe_move(file_path, dest_path):
            logger.info(f"→ Moved to duplicates: {dest_path.relative_to(Path.home())}")
            return True
        return False
    
    def match_rule(self, file_path: Path, rule: SortingRule) -> Tuple[bool, str]:
        """
        Checks if file matches a rule.
        Returns (matched, reason).
        """
        file_suffix = file_path.suffix.lower()
        file_name_lower = file_path.name.lower()
        
        # Check extensions
        if rule.extensions and file_suffix not in rule.extensions:
            return False, "extension mismatch"
        
        # Check filename keywords
        if rule.name_contains:
            if not any(kw in file_name_lower for kw in rule.name_contains):
                return False, "name keywords not found"
        
        # Check PDF content keywords
        if rule.content_contains:
            if file_suffix != '.pdf':
                return False, "not a PDF"
            
            content = self.pdf_analyzer.extract_text(file_path, max_pages=10)
            if not content:
                return False, "PDF content extraction failed"
            
            has_match, matched_kw = self.pdf_analyzer.contains_keywords(
                content, rule.content_contains
            )
            if not has_match:
                return False, "content keywords not found"
            
            logger.info(f"Content match: '{matched_kw}' found in '{file_path.name}'")
        
        return True, "all criteria matched"
    
    def sort_file(self, file_path: Path):
        """Main sorting function with comprehensive error handling."""
        try:
            # Wait for file to be fully written
            if not self.file_ops.wait_for_file_stability(file_path):
                logger.debug(f"File disappeared: '{file_path.name}'")
                return
            
            # Calculate hash
            file_hash = self.db.get_file_hash(file_path)
            if not file_hash:
                logger.warning(f"Skipping '{file_path.name}' - cannot compute hash")
                return
            
            # Check for duplicates
            if self.db.is_duplicate(file_hash):
                self.handle_duplicate(file_path, file_hash)
                return
            
            # Try to match against rules
            for rule in self.config.rules:
                matched, reason = self.match_rule(file_path, rule)
                
                if matched:
                    dest_path = self.file_ops.get_unique_path(
                        rule.dest / file_path.name
                    )
                    
                    if self.file_ops.safe_move(file_path, dest_path):
                        self.db.add_file_record(file_hash, dest_path)
                        logger.info(
                            f"✓ [{rule.name}] '{file_path.name}' → "
                            f"{dest_path.relative_to(Path.home())}"
                        )
                        return
            
            # No rule matched - log hash to prevent reprocessing
            self.db.add_file_record(file_hash, file_path)
            logger.info(f"No matching rule for '{file_path.name}' (hash: {file_hash[:12]}...)")
            
        except FileNotFoundError:
            logger.debug(f"File vanished during processing: '{file_path.name}'")
        except Exception as e:
            logger.error(f"Error processing '{file_path.name}': {e}", exc_info=True)


# --- Configuration Loader ---

def load_config(config_path: str = CONFIG_FILE) -> Config:
    """Loads and validates configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            raw_config = yaml.safe_load(f)
        
        # Expand paths
        source_folder = Path(raw_config['source_folder']).expanduser().resolve()
        duplicate_folder = Path(raw_config['duplicate_folder']).expanduser().resolve()
        
        # Parse rules
        rules = []
        for rule_data in raw_config.get('rules', []):
            rule = SortingRule(
                name=rule_data['name'],
                dest=Path(rule_data['dest']).expanduser().resolve(),
                extensions=rule_data.get('extensions'),
                name_contains=rule_data.get('name_contains'),
                content_contains=rule_data.get('content_contains'),
                rule_type=rule_data.get('type')
            )
            rules.append(rule)
        
        logger.info(f"Loaded {len(rules)} rules from '{config_path}'")
        return Config(source_folder, duplicate_folder, rules)
        
    except FileNotFoundError:
        logger.error(f"Configuration file not found: '{config_path}'")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in '{config_path}': {e}")
        sys.exit(1)
    except KeyError as e:
        logger.error(f"Missing required configuration key: {e}")
        sys.exit(1)


# --- Watchdog Event Handler ---

class SortingEventHandler(FileSystemEventHandler):
    """Monitors filesystem events and triggers sorting."""
    
    def __init__(self, sorter: FileSorter):
        super().__init__()
        self.sorter = sorter
    
    def on_created(self, event: FileSystemEvent):
        """Handles file creation events."""
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        if self.sorter.should_ignore_file(file_path):
            logger.debug(f"Ignored: '{file_path.name}'")
            return
        
        self.sorter.sort_file(file_path)


# --- Main Execution ---

def main():
    """Application entry point."""
    logger.info("=" * 60)
    logger.info("CORTEX SORTER [Phase 2] - Starting...")
    logger.info("=" * 60)
    
    # Initialize components
    db_manager = DatabaseManager()
    config = load_config()
    
    # Validate source folder
    if not config.source_folder.exists():
        logger.error(f"Source folder does not exist: '{config.source_folder}'")
        sys.exit(1)
    
    # Create duplicate folder
    config.duplicate_folder.mkdir(parents=True, exist_ok=True)
    
    # Initialize sorter
    sorter = FileSorter(config, db_manager)
    
    # Setup file system monitoring
    event_handler = SortingEventHandler(sorter)
    observer = Observer()
    observer.schedule(event_handler, str(config.source_folder), recursive=False)
    
    observer.start()
    logger.info(f"Watching: {config.source_folder}")
    logger.info(f"Duplicates: {config.duplicate_folder}")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)
    
    try:
        while observer.is_alive():
            observer.join(timeout=1)
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info("Stopping Cortex Sorter...")
        observer.stop()
    
    observer.join()
    logger.info("Cortex Sorter stopped successfully")


if __name__ == "__main__":
    main()