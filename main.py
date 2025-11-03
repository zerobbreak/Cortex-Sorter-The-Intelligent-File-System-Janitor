import sys
import time
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Union
from dataclasses import dataclass, field
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

# --- Custom Exceptions ---

class CortexSorterError(Exception):
    """Base exception for Cortex Sorter errors."""
    pass

class ConfigurationError(CortexSorterError):
    """Raised when there's an issue with configuration."""
    pass

class DatabaseError(CortexSorterError):
    """Raised when there's an issue with database operations."""
    pass

class FileProcessingError(CortexSorterError):
    """Raised when there's an issue processing a file."""
    pass

class PDFProcessingError(FileProcessingError):
    """Raised when there's an issue processing a PDF file."""
    pass

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
    priority: int = field(default=0, init=False)

    def __post_init__(self):
        """Validate and normalize rule data."""
        if not self.name or not self.name.strip():
            raise ConfigurationError("Rule name cannot be empty")

        if not self.dest:
            raise ConfigurationError(f"Rule '{self.name}' must have a destination path")

        # Normalize extensions to lowercase with leading dots
        if self.extensions:
            if not isinstance(self.extensions, list):
                raise ConfigurationError(f"Rule '{self.name}': extensions must be a list")
            self.extensions = [
                ext.lower() if ext.startswith('.') else f'.{ext.lower()}'
                for ext in self.extensions
            ]

        # Normalize keywords to lowercase for case-insensitive matching
        if self.name_contains:
            if not isinstance(self.name_contains, list):
                raise ConfigurationError(f"Rule '{self.name}': name_contains must be a list")
            self.name_contains = [kw.lower().strip() for kw in self.name_contains if kw.strip()]

        if self.content_contains:
            if not isinstance(self.content_contains, list):
                raise ConfigurationError(f"Rule '{self.name}': content_contains must be a list")
            self.content_contains = [kw.lower().strip() for kw in self.content_contains if kw.strip()]

        # Set priority based on specificity (more criteria = higher priority)
        criteria_count = sum([
            1 if self.extensions else 0,
            1 if self.name_contains else 0,
            1 if self.content_contains else 0
        ])
        self.priority = criteria_count

    def is_valid_for_file(self, file_path: Path) -> bool:
        """Check if this rule has any criteria that could match the file."""
        if not file_path.exists():
            return False

        file_suffix = file_path.suffix.lower()

        # If rule specifies extensions, file must match at least one
        if self.extensions and file_suffix not in self.extensions:
            return False

        # If rule specifies name keywords, check if filename contains any
        if self.name_contains:
            file_name_lower = file_path.name.lower()
            if not any(kw in file_name_lower for kw in self.name_contains):
                return False

        # Content matching requires PDF files and is checked separately
        if self.content_contains and file_suffix != '.pdf':
            return False

        return True


@dataclass
class Config:
    """Application configuration."""
    source_folder: Path
    duplicate_folder: Path
    rules: List[SortingRule]

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.source_folder:
            raise ConfigurationError("Source folder must be specified")

        if not self.duplicate_folder:
            raise ConfigurationError("Duplicate folder must be specified")

        if self.source_folder == self.duplicate_folder:
            raise ConfigurationError("Source and duplicate folders cannot be the same")

        if not self.rules:
            logger.warning("No sorting rules defined - all files will be ignored")

        # Sort rules by priority (most specific first)
        self.rules.sort(key=lambda r: r.priority, reverse=True)

    def validate_paths(self) -> None:
        """Validate that configured paths exist and are accessible."""
        try:
            if not self.source_folder.exists():
                raise ConfigurationError(f"Source folder does not exist: {self.source_folder}")

            if not self.source_folder.is_dir():
                raise ConfigurationError(f"Source path is not a directory: {self.source_folder}")

            # Create duplicate folder if it doesn't exist
            self.duplicate_folder.mkdir(parents=True, exist_ok=True)

            # Validate rule destinations
            for rule in self.rules:
                try:
                    rule.dest.mkdir(parents=True, exist_ok=True)
                except (OSError, PermissionError) as e:
                    logger.warning(f"Cannot create destination folder for rule '{rule.name}': {e}")

        except (OSError, PermissionError) as e:
            raise ConfigurationError(f"Path validation failed: {e}")


# --- Database Module with Connection Pooling ---

class DatabaseManager:
    """Manages database operations with improved error handling."""

    def __init__(self, db_path: str = DB_NAME):
        self.db_path = Path(db_path)
        self._init_database()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=10.0)
            conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
            conn.execute("PRAGMA synchronous=NORMAL")  # Balance performance and safety
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to connect to database: {e}")

        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            if isinstance(e, sqlite3.Error):
                raise DatabaseError(f"Database operation failed: {e}")
            raise
        finally:
            conn.close()

    def _init_database(self):
        """Creates the database schema if it doesn't exist."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Create main table
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

                # Create indexes for better performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sha256_hash
                    ON file_hashes(sha256_hash);
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_file_name
                    ON file_hashes(file_name);
                """)

                # Check database integrity
                cursor.execute("PRAGMA integrity_check")
                result = cursor.fetchone()
                if result and result[0] != "ok":
                    raise DatabaseError(f"Database integrity check failed: {result[0]}")

            logger.info(f"Database '{self.db_path}' initialized successfully")

        except Exception as e:
            raise DatabaseError(f"Failed to initialize database: {e}")
    
    def get_file_hash(self, file_path: Path) -> Optional[str]:
        """
        Calculates SHA-256 hash with improved error handling and performance.
        Returns None if file cannot be hashed.
        """
        hash_obj = HASH_ALGORITHM()
        try:
            # Check if file exists and is readable
            if not file_path.exists():
                logger.debug(f"File does not exist: {file_path}")
                return None

            if not file_path.is_file():
                logger.debug(f"Path is not a file: {file_path}")
                return None

            file_size = file_path.stat().st_size

            # Skip empty files
            if file_size == 0:
                logger.debug(f"Skipping empty file: {file_path}")
                return None

            with open(file_path, 'rb') as f:
                while chunk := f.read(CHUNK_SIZE):
                    hash_obj.update(chunk)

            return hash_obj.hexdigest()

        except (IOError, OSError, PermissionError) as e:
            logger.warning(f"Cannot hash '{file_path.name}': {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error hashing '{file_path.name}': {e}")
            return None
    
    def is_duplicate(self, file_hash: str) -> bool:
        """Checks if hash exists in database."""
        if not file_hash or not file_hash.strip():
            return False

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT 1 FROM file_hashes WHERE sha256_hash = ? LIMIT 1",
                    (file_hash.strip(),)
                )
                return cursor.fetchone() is not None
        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to check for duplicate: {e}")
    
    def add_file_record(self, file_hash: str, file_path: Path) -> bool:
        """
        Adds file record to database with additional metadata.
        Returns True if added, False if already exists.
        """
        if not file_hash or not file_hash.strip():
            return False

        try:
            file_size = file_path.stat().st_size
        except (OSError, FileNotFoundError):
            file_size = None

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO file_hashes
                        (sha256_hash, original_path, file_size, file_name)
                        VALUES (?, ?, ?, ?)
                    """, (file_hash.strip(), str(file_path), file_size, file_path.name))
                    logger.debug(f"Added hash record: {file_hash[:12]}... for {file_path.name}")
                    return True
                except sqlite3.IntegrityError:
                    logger.debug(f"Hash {file_hash[:12]}... already exists")
                    return False
        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to add file record: {e}")

    def get_statistics(self) -> Dict[str, int]:
        """Get database statistics."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM file_hashes")
                total_files = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(DISTINCT sha256_hash) FROM file_hashes")
                unique_files = cursor.fetchone()[0]

                duplicate_count = total_files - unique_files

                cursor.execute("SELECT SUM(file_size) FROM file_hashes WHERE file_size IS NOT NULL")
                total_size = cursor.fetchone()[0] or 0

                return {
                    'total_files': total_files,
                    'unique_files': unique_files,
                    'duplicates': duplicate_count,
                    'total_size_bytes': total_size
                }
        except Exception as e:
            raise DatabaseError(f"Failed to get statistics: {e}")
    
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
        if not file_path.exists() or not file_path.is_file():
            logger.debug(f"PDF file does not exist or is not a file: {file_path}")
            return None

        if file_path.suffix.lower() != '.pdf':
            logger.debug(f"File is not a PDF: {file_path}")
            return None

        try:
            content_parts = []

            with pdfplumber.open(file_path) as pdf:
                if not pdf.pages:
                    logger.debug(f"PDF has no pages: {file_path}")
                    return None

                pages_to_process = pdf.pages[:max_pages] if max_pages else pdf.pages
                logger.debug(f"Processing {len(pages_to_process)} pages from {file_path.name}")

                for i, page in enumerate(pages_to_process):
                    try:
                        text = page.extract_text()
                        if text and text.strip():
                            content_parts.append(text.strip())
                    except Exception as e:
                        logger.warning(f"Failed to extract text from page {i+1} of {file_path.name}: {e}")
                        continue

            if not content_parts:
                logger.debug(f"No text content extracted from {file_path.name}")
                return None

            return ' '.join(content_parts)

        except Exception as e:
            # Handle various PDF-related exceptions generically
            error_msg = str(e).lower()
            if "encrypted" in error_msg or "password" in error_msg:
                logger.warning(f"PDF is encrypted and cannot be read: '{file_path.name}'")
            elif "syntax" in error_msg or "format" in error_msg:
                logger.warning(f"PDF syntax/format error in '{file_path.name}': {e}")
            else:
                logger.warning(f"PDF extraction failed for '{file_path.name}': {e}")
            return None

    @staticmethod
    def contains_keywords(content: str, keywords: List[str]) -> Tuple[bool, Optional[str]]:
        """
        Checks if content contains any of the keywords.
        Returns (match_found, matched_keyword).
        """
        if not content or not content.strip():
            return False, None

        if not keywords:
            return False, None

        content_lower = content.lower()
        for keyword in keywords:
            if not keyword or not keyword.strip():
                continue

            keyword_lower = keyword.lower().strip()
            if keyword_lower in content_lower:
                return True, keyword

        return False, None

    @staticmethod
    def get_pdf_info(file_path: Path) -> Optional[Dict[str, Union[int, str]]]:
        """Get basic information about a PDF file."""
        if not file_path.exists() or file_path.suffix.lower() != '.pdf':
            return None

        try:
            with pdfplumber.open(file_path) as pdf:
                return {
                    'pages': len(pdf.pages),
                    'metadata': pdf.metadata or {}
                }
        except Exception as e:
            logger.debug(f"Could not get PDF info for {file_path.name}: {e}")
            return None


# --- File System Operations ---

class FileOperations:
    """Handles file system operations with conflict resolution."""

    @staticmethod
    def get_unique_path(destination_path: Path, max_attempts: int = 1000) -> Path:
        """
        Generates unique path by appending counter if conflicts exist.
        Raises FileProcessingError if max_attempts reached.
        """
        if not destination_path.exists():
            return destination_path

        for counter in range(1, max_attempts):
            new_path = destination_path.with_name(
                f"{destination_path.stem}_{counter}{destination_path.suffix}"
            )
            if not new_path.exists():
                return new_path

        raise FileProcessingError(
            f"Could not generate unique path after {max_attempts} attempts for: {destination_path}"
        )

    @staticmethod
    def wait_for_file_stability(file_path: Path, delay: float = FILE_STABILITY_DELAY) -> bool:
        """
        Waits for file to finish writing by checking size stability.
        Returns True if file is stable, False if it disappeared.
        """
        if not file_path.exists():
            return False

        try:
            # Get initial file size
            initial_size = file_path.stat().st_size
            initial_mtime = file_path.stat().st_mtime

            time.sleep(delay)

            if not file_path.exists():
                return False

            # Check if file size and modification time are stable
            current_size = file_path.stat().st_size
            current_mtime = file_path.stat().st_mtime

            if initial_size == current_size and initial_mtime == current_mtime:
                # Try to open file to ensure it's not locked
                try:
                    with open(file_path, 'rb') as f:
                        f.read(1)  # Read a small amount to test access
                    return True
                except (IOError, PermissionError):
                    # File might still be writing
                    time.sleep(delay)
                    return file_path.exists() and file_path.stat().st_size == current_size

            # File is still changing, wait a bit more
            time.sleep(delay)
            return file_path.exists()

        except (OSError, FileNotFoundError):
            return False

    @staticmethod
    def safe_move(source: Path, destination: Path) -> bool:
        """
        Safely moves file with error handling.
        Returns True on success, False on failure.
        """
        try:
            # Ensure destination directory exists
            destination.parent.mkdir(parents=True, exist_ok=True)

            # Use rename for atomic move
            source.rename(destination)
            logger.debug(f"Successfully moved {source.name} to {destination}")
            return True

        except (OSError, PermissionError) as e:
            logger.error(f"Failed to move '{source.name}' to '{destination}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error moving '{source.name}': {e}")
            return False

    @staticmethod
    def safe_copy(source: Path, destination: Path) -> bool:
        """
        Safely copies file with error handling.
        Returns True on success, False on failure.
        """
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            import shutil
            shutil.copy2(source, destination)
            logger.debug(f"Successfully copied {source.name} to {destination}")
            return True
        except (OSError, PermissionError) as e:
            logger.error(f"Failed to copy '{source.name}' to '{destination}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error copying '{source.name}': {e}")
            return False


# --- Core Sorting Engine ---

class FileSorter:
    """Main file sorting logic with rule matching."""

    def __init__(self, config: Config, db_manager: DatabaseManager):
        self.config = config
        self.db = db_manager
        self.pdf_analyzer = PDFAnalyzer()
        self.file_ops = FileOperations()
        self.stats = {
            'files_processed': 0,
            'duplicates_found': 0,
            'files_sorted': 0,
            'errors': 0
        }

    def should_ignore_file(self, file_path: Path) -> bool:
        """Determines if file should be ignored based on filename patterns."""
        if not file_path.exists():
            return True

        name = file_path.name.lower()

        # Ignore hidden files and system files
        ignored_patterns = ('.', '~', 'thumbs.db', '.ds_store', 'desktop.ini')

        # Ignore temporary and download files
        ignored_extensions = ('.tmp', '.crdownload', '.part', '.download', '.swp', '.bak')

        # Ignore files that start with ignored patterns
        if any(name.startswith(pattern) for pattern in ignored_patterns):
            logger.debug(f"Ignoring hidden/system file: {file_path.name}")
            return True

        # Ignore files with ignored extensions
        if file_path.suffix.lower() in ignored_extensions:
            logger.debug(f"Ignoring temp file: {file_path.name}")
            return True

        return False
    
    def handle_duplicate(self, file_path: Path, file_hash: str) -> bool:
        """
        Moves duplicate file to duplicate folder.
        Returns True if successfully handled.
        """
        try:
            dup_info = self.db.get_duplicate_info(file_hash)
            original_path = dup_info[0] if dup_info else "unknown"
            original_name = Path(original_path).name if original_path != "unknown" else "unknown"

            logger.info(f"Duplicate detected: '{file_path.name}' (original: {original_name})")

            # Increment counter when duplicate is detected
            self.stats['duplicates_found'] += 1

            dest_path = self.file_ops.get_unique_path(
                self.config.duplicate_folder / file_path.name
            )

            if self.file_ops.safe_move(file_path, dest_path):
                try:
                    relative_path = dest_path.relative_to(Path.home())
                    logger.info(f"→ Moved duplicate to: {relative_path}")
                except ValueError:
                    # Path is not relative to home, use absolute path
                    logger.info(f"→ Moved duplicate to: {dest_path}")
                return True
            else:
                logger.error(f"Failed to move duplicate: {file_path.name}")
                return False

        except Exception as e:
            logger.error(f"Error handling duplicate '{file_path.name}': {e}")
            return False
    
    def match_rule(self, file_path: Path, rule: SortingRule) -> Tuple[bool, str]:
        """
        Checks if file matches a rule.
        Returns (matched, reason).
        """
        try:
            # Check if file exists
            if not file_path.exists():
                return False, "file does not exist"

            file_suffix = file_path.suffix.lower()
            file_name_lower = file_path.name.lower()

            # Check extensions first (fastest check)
            if rule.extensions:
                if file_suffix not in rule.extensions:
                    return False, f"extension '{file_suffix}' not in {rule.extensions}"
                logger.debug(f"Extension match for rule '{rule.name}': {file_suffix}")

            # Check filename keywords (second fastest)
            if rule.name_contains:
                matching_keywords = [kw for kw in rule.name_contains if kw in file_name_lower]
                if not matching_keywords:
                    return False, f"name keywords {rule.name_contains} not found in filename"
                logger.debug(f"Name match for rule '{rule.name}': {matching_keywords}")

            # Check PDF content keywords (slowest, only for PDFs)
            if rule.content_contains:
                if file_suffix != '.pdf':
                    return False, f"content search requires PDF file, got {file_suffix}"

                logger.debug(f"Extracting content from PDF: {file_path.name}")
                content = self.pdf_analyzer.extract_text(file_path, max_pages=10)
                if not content:
                    return False, "PDF content extraction failed or returned no text"

                has_match, matched_kw = self.pdf_analyzer.contains_keywords(
                    content, rule.content_contains
                )
                if not has_match:
                    return False, f"content keywords {rule.content_contains} not found in PDF"

                logger.info(f"✓ Content match: '{matched_kw}' found in '{file_path.name}' (rule: {rule.name})")

            return True, "all criteria matched"

        except Exception as e:
            logger.error(f"Error matching rule '{rule.name}' against '{file_path.name}': {e}")
            return False, f"matching error: {e}"
    
    def sort_file(self, file_path: Path):
        """Main sorting function with comprehensive error handling."""
        self.stats['files_processed'] += 1

        try:
            # Step 1: Validate and prepare file
            if not self._prepare_file_for_sorting(file_path):
                return

            # Step 2: Calculate file hash
            file_hash = self._calculate_file_hash(file_path)
            if not file_hash:
                return

            # Step 3: Check for duplicates
            if self._handle_duplicate_if_exists(file_path, file_hash):
                return

            # Step 4: Try to match against sorting rules
            if self._apply_sorting_rules(file_path, file_hash):
                self.stats['files_sorted'] += 1
                return

            # Step 5: No rule matched - keep file but record hash
            self._handle_unmatched_file(file_path, file_hash)

        except (FileNotFoundError, FileProcessingError) as e:
            logger.debug(f"File processing skipped for '{file_path.name}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing '{file_path.name}': {e}", exc_info=True)
            self.stats['errors'] += 1

    def _prepare_file_for_sorting(self, file_path: Path) -> bool:
        """Prepare file for sorting - check stability and existence."""
        if not file_path.exists():
            logger.debug(f"File no longer exists: {file_path}")
            return False

        # Wait for file to be fully written
        if not self.file_ops.wait_for_file_stability(file_path):
            logger.debug(f"File disappeared or is unstable: '{file_path.name}'")
            return False

        return True

    def _calculate_file_hash(self, file_path: Path) -> Optional[str]:
        """Calculate and return file hash, handling errors gracefully."""
        file_hash = self.db.get_file_hash(file_path)
        if not file_hash:
            logger.warning(f"Skipping '{file_path.name}' - cannot compute hash")
            return None
        return file_hash

    def _handle_duplicate_if_exists(self, file_path: Path, file_hash: str) -> bool:
        """Check for duplicates and handle if found. Returns True if duplicate was handled."""
        try:
            if self.db.is_duplicate(file_hash):
                return self.handle_duplicate(file_path, file_hash)
        except DatabaseError as e:
            logger.error(f"Database error checking for duplicates: {e}")
        return False

    def _apply_sorting_rules(self, file_path: Path, file_hash: str) -> bool:
        """Apply sorting rules to file. Returns True if file was sorted."""
        for rule in self.config.rules:
            try:
                matched, reason = self.match_rule(file_path, rule)

                if matched:
                    return self._move_file_with_rule(file_path, file_hash, rule)

            except Exception as e:
                logger.warning(f"Error applying rule '{rule.name}' to '{file_path.name}': {e}")
                continue

        return False

    def _move_file_with_rule(self, file_path: Path, file_hash: str, rule: SortingRule) -> bool:
        """Move file according to rule. Returns True on success."""
        try:
            dest_path = self.file_ops.get_unique_path(rule.dest / file_path.name)

            if self.file_ops.safe_move(file_path, dest_path):
                self.db.add_file_record(file_hash, dest_path)
                try:
                    relative_path = dest_path.relative_to(Path.home())
                    logger.info(
                        f"✓ [{rule.name}] '{file_path.name}' → "
                        f"{relative_path}"
                    )
                except ValueError:
                    # Path is not relative to home, use absolute path
                    logger.info(
                        f"✓ [{rule.name}] '{file_path.name}' → "
                        f"{dest_path}"
                    )
                return True
            else:
                logger.error(f"Failed to move '{file_path.name}' with rule '{rule.name}'")
                return False

        except Exception as e:
            logger.error(f"Error moving file with rule '{rule.name}': {e}")
            return False

    def _handle_unmatched_file(self, file_path: Path, file_hash: str):
        """Handle file that didn't match any rules."""
        try:
            self.db.add_file_record(file_hash, file_path)
            logger.info(f"No matching rule for '{file_path.name}' (hash: {file_hash[:12]}...)")
        except DatabaseError as e:
            logger.error(f"Failed to record unmatched file '{file_path.name}': {e}")


# --- Configuration Loader ---

def load_config(config_path: str = CONFIG_FILE) -> Config:
    """Loads and validates configuration from YAML file."""
    config_path_obj = Path(config_path)

    try:
        # Check if config file exists
        if not config_path_obj.exists():
            raise ConfigurationError(f"Configuration file not found: '{config_path}'")

        if not config_path_obj.is_file():
            raise ConfigurationError(f"Configuration path is not a file: '{config_path}'")

        # Load and parse YAML
        with open(config_path_obj, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)

        if raw_config is None:
            raise ConfigurationError(f"Configuration file is empty: '{config_path}'")

        if not isinstance(raw_config, dict):
            raise ConfigurationError(f"Configuration root must be a dictionary, got {type(raw_config)}")

        # Validate required top-level keys
        required_keys = ['source_folder', 'duplicate_folder']
        for key in required_keys:
            if key not in raw_config:
                raise ConfigurationError(f"Missing required configuration key: '{key}'")

        # Expand and resolve paths
        source_folder = _expand_and_validate_path(raw_config['source_folder'], "source_folder")
        duplicate_folder = _expand_and_validate_path(raw_config['duplicate_folder'], "duplicate_folder")

        # Parse and validate rules
        rules = _parse_sorting_rules(raw_config.get('rules', []))

        logger.info(f"Successfully loaded configuration: {len(rules)} rules from '{config_path}'")
        return Config(source_folder, duplicate_folder, rules)

    except ConfigurationError:
        raise  # Re-raise configuration errors
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Invalid YAML in '{config_path}': {e}")
    except (IOError, OSError) as e:
        raise ConfigurationError(f"Error reading configuration file '{config_path}': {e}")
    except Exception as e:
        raise ConfigurationError(f"Unexpected error loading configuration: {e}")

def _expand_and_validate_path(path_str: str, field_name: str) -> Path:
    """Expand user path and validate it exists."""
    if not path_str or not path_str.strip():
        raise ConfigurationError(f"{field_name} cannot be empty")

    try:
        path = Path(path_str).expanduser().resolve()
        return path
    except Exception as e:
        raise ConfigurationError(f"Invalid path for {field_name} '{path_str}': {e}")

def _parse_sorting_rules(raw_rules: list) -> List[SortingRule]:
    """Parse and validate sorting rules from configuration."""
    if not isinstance(raw_rules, list):
        raise ConfigurationError("Rules must be a list")

    rules = []
    seen_names = set()

    for i, rule_data in enumerate(raw_rules):
        try:
            if not isinstance(rule_data, dict):
                raise ConfigurationError(f"Rule {i+1} must be a dictionary")

            if 'name' not in rule_data:
                raise ConfigurationError(f"Rule {i+1} missing required 'name' field")

            rule_name = rule_data['name']
            if not rule_name or not rule_name.strip():
                raise ConfigurationError(f"Rule {i+1} has empty name")

            if rule_name in seen_names:
                raise ConfigurationError(f"Duplicate rule name: '{rule_name}'")

            seen_names.add(rule_name)

            if 'dest' not in rule_data:
                raise ConfigurationError(f"Rule '{rule_name}' missing required 'dest' field")

            dest_path = _expand_and_validate_path(rule_data['dest'], f"rule '{rule_name}' destination")

            # Create rule with validation
            rule = SortingRule(
                name=rule_name.strip(),
                dest=dest_path,
                extensions=rule_data.get('extensions'),
                name_contains=rule_data.get('name_contains'),
                content_contains=rule_data.get('content_contains'),
                rule_type=rule_data.get('type')
            )

            rules.append(rule)
            logger.debug(f"Parsed rule: {rule_name} (priority: {rule.priority})")

        except ConfigurationError:
            raise  # Re-raise configuration errors with context
        except Exception as e:
            raise ConfigurationError(f"Error parsing rule {i+1}: {e}")

    return rules


# --- Watchdog Event Handler ---

class SortingEventHandler(FileSystemEventHandler):
    """Monitors filesystem events and triggers sorting."""

    def __init__(self, sorter: FileSorter):
        super().__init__()
        self.sorter = sorter

    def on_created(self, event: FileSystemEvent):
        """Handles file creation events."""
        if event.is_directory:
            logger.debug("Ignoring directory creation event")
            return

        file_path = Path(event.src_path)

        if self.sorter.should_ignore_file(file_path):
            logger.debug(f"Ignored file: '{file_path.name}'")
            return

        logger.debug(f"Processing new file: {file_path.name}")
        self.sorter.sort_file(file_path)

    def on_moved(self, event: FileSystemEvent):
        """Handles file move events within watched directory."""
        if event.is_directory:
            return

        # Only process if destination is in watched directory
        dest_path = Path(event.dest_path)
        if dest_path.parent == self.sorter.config.source_folder:
            if self.sorter.should_ignore_file(dest_path):
                logger.debug(f"Ignored moved file: '{dest_path.name}'")
                return

            logger.debug(f"Processing moved file: {dest_path.name}")
            self.sorter.sort_file(dest_path)


# --- Main Execution ---

def main():
    """Application entry point."""
    logger.info("=" * 60)
    logger.info("CORTEX SORTER [Phase 2] - Starting...")
    logger.info("=" * 60)

    observer = None
    sorter = None

    try:
        # Initialize components
        db_manager = DatabaseManager()
        config = load_config()

        # Validate and prepare paths
        config.validate_paths()

        # Initialize sorter
        sorter = FileSorter(config, db_manager)

        # Log configuration summary
        _log_startup_summary(config, db_manager)

        # Setup file system monitoring
        event_handler = SortingEventHandler(sorter)
        observer = Observer()
        observer.schedule(event_handler, str(config.source_folder), recursive=False)

        observer.start()
        logger.info("=" * 60)
        logger.info("Cortex Sorter is now running!")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)

        # Main monitoring loop
        while observer.is_alive():
            observer.join(timeout=1)

            # Periodic statistics logging (every 5 minutes)
            # This is a simple implementation - could be enhanced with threading

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 60)
        logger.info("Stopping Cortex Sorter...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Graceful shutdown
        if observer:
            try:
                observer.stop()
                observer.join(timeout=5)
                logger.info("File system monitoring stopped")
            except Exception as e:
                logger.error(f"Error stopping observer: {e}")

        # Log final statistics
        if sorter:
            _log_final_statistics(sorter, db_manager)

        logger.info("Cortex Sorter stopped successfully")

def _log_startup_summary(config: Config, db_manager: DatabaseManager):
    """Log configuration and system status at startup."""
    logger.info(f"Watching folder: {config.source_folder}")
    logger.info(f"Duplicates folder: {config.duplicate_folder}")
    logger.info(f"Number of rules: {len(config.rules)}")

    # Log database statistics
    try:
        stats = db_manager.get_statistics()
        logger.info(f"Database: {stats['total_files']} files, {stats['unique_files']} unique, "
                   f"{stats['duplicates']} duplicates")
        if stats['total_size_bytes'] > 0:
            size_mb = stats['total_size_bytes'] / (1024 * 1024)
            logger.info(f"Total processed size: {size_mb:.1f} MB")
    except Exception as e:
        logger.warning(f"Could not load database statistics: {e}")

    # Log rules summary
    rule_types = {}
    for rule in config.rules:
        rule_type = rule.rule_type or 'general'
        rule_types[rule_type] = rule_types.get(rule_type, 0) + 1

    logger.info(f"Rule types: {rule_types}")

def _log_final_statistics(sorter: FileSorter, db_manager: DatabaseManager):
    """Log final statistics before shutdown."""
    try:
        logger.info("\n" + "=" * 40)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 40)

        # Sorter statistics
        stats = sorter.stats
        logger.info(f"Files processed: {stats['files_processed']}")
        logger.info(f"Files sorted: {stats['files_sorted']}")
        logger.info(f"Duplicates found: {stats['duplicates_found']}")
        logger.info(f"Errors encountered: {stats['errors']}")

        # Database statistics
        db_stats = db_manager.get_statistics()
        logger.info(f"Total files in database: {db_stats['total_files']}")
        logger.info(f"Unique files: {db_stats['unique_files']}")
        logger.info(f"Duplicates stored: {db_stats['duplicates']}")

        if db_stats['total_size_bytes'] > 0:
            size_mb = db_stats['total_size_bytes'] / (1024 * 1024)
            logger.info(f"Total processed size: {size_mb:.1f} MB")

        logger.info("=" * 40)

    except Exception as e:
        logger.error(f"Error logging final statistics: {e}")


if __name__ == "__main__":
    main()