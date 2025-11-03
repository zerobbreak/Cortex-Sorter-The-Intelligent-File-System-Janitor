# 🤖 Cortex Sorter - The Intelligent File System Janitor

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-110%20passed-success.svg)](tests/)

**Cortex Sorter** is an intelligent file management system that automatically monitors, sorts, and organizes your files based on customizable rules. It uses advanced content analysis, duplicate detection, and machine learning principles to keep your digital workspace clean and organized.

## ✨ Features

### 🧠 Intelligent File Analysis
- **PDF Content Analysis**: Scans PDF documents for keywords and content patterns
- **Filename Pattern Matching**: Intelligent keyword detection in filenames
- **File Type Recognition**: Automatic detection of file types and extensions

### 🔄 Real-Time Monitoring
- **Watchdog Integration**: Monitors folders in real-time using filesystem events
- **Automatic Processing**: Instantly sorts new files as they arrive
- **Stability Checking**: Ensures files are fully written before processing

### 🚫 Duplicate Detection
- **SHA-256 Hashing**: Cryptographic file hashing for accurate duplicate detection
- **Persistent Database**: SQLite-based storage of file signatures
- **Automatic Cleanup**: Moves duplicates to dedicated folders

### 📋 Flexible Rule Engine
- **YAML Configuration**: Human-readable, version-controlled rules
- **Multi-Criteria Matching**: Combine file type, name, and content criteria
- **Priority-Based Sorting**: Rules are automatically prioritized by specificity

### 📊 Comprehensive Statistics
- **Processing Metrics**: Track files processed, sorted, and duplicates found
- **Database Analytics**: Monitor storage usage and file counts
- **Detailed Logging**: Full audit trail with configurable log levels

## 🚀 Quick Start

### Installation

1. **Clone the repository:**
```bash
git clone https://github.com/yourusername/cortex-sorter.git
cd cortex-sorter
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Configure your rules:**
Edit `rules.yml` to customize sorting behavior for your needs.

4. **Run the sorter:**
```bash
python main.py
```

**Note:** The application creates a `file_hashes.db` SQLite database to track processed files and detect duplicates.

## ⚙️ Configuration

### Basic Setup

Create a `rules.yml` file in the project directory:

```yaml
# Core folders to monitor
source_folder: "~/Downloads"           # Folder to watch for new files
duplicate_folder: "~/Downloads/_Duplicates"  # Where duplicates go

# Sorting rules (processed top-to-bottom, first match wins)
rules:
  - name: "Tax Documents"
    extensions: [".pdf"]
    content_contains: ["Tax Return", "IRS", "W-2"]
    dest: "~/Documents/Finance/Taxes"

  - name: "Invoices"
    extensions: [".pdf"]
    content_contains: ["Invoice", "Amount Due"]
    dest: "~/Documents/Finance/Invoices"

  - name: "Screenshots"
    extensions: [".png", ".jpg"]
    name_contains: ["screenshot", "screen shot"]
    dest: "~/Pictures/Screenshots"
```

### Rule Criteria

Each rule supports three types of matching criteria:

| Criterion | Description | Example |
|-----------|-------------|---------|
| `extensions` | File extensions to match | `[.pdf, .docx]` |
| `name_contains` | Keywords in filename | `["invoice", "receipt"]` |
| `content_contains` | Keywords in PDF content | `["Tax Return", "IRS"]` |

**All criteria in a rule must match (AND logic).** Rules are automatically sorted by specificity (more criteria = higher priority).

### Advanced Configuration

```yaml
# Full example with all options
rules:
  - name: "Confidential Reports"
    type: "document"                    # Optional: for categorization
    extensions: [".pdf", ".docx"]
    name_contains: ["confidential", "internal"]
    content_contains: ["CONFIDENTIAL", "Internal Use Only"]
    dest: "~/Documents/Work/Confidential"

  - name: "Meeting Notes"
    extensions: [".txt", ".md", ".pdf"]
    name_contains: ["meeting", "notes", "minutes"]
    dest: "~/Documents/Work/Meetings"
```

## 📖 Usage

### Command Line Interface

```bash
# Start monitoring with default configuration (rules.yml)
python main.py

# The application will:
# 1. Load rules.yml from the current directory
# 2. Start monitoring the configured source folder
# 3. Display real-time sorting activity
# 4. Press Ctrl+C to stop gracefully
```

### Programmatic Usage

```python
from main import FileSorter, DatabaseManager, load_config

# Load configuration
config = load_config("rules.yml")

# Initialize components
db = DatabaseManager()
sorter = FileSorter(config, db)

# Sort a specific file
from pathlib import Path
sorter.sort_file(Path("~/Downloads/new_invoice.pdf"))
```

## 🧪 Testing

### Run the Test Suite

```bash
# Run all tests with coverage (recommended)
python tests/run_tests.py

# Or use pytest directly
python -m pytest tests/ --cov=main --cov-report=html

# Run specific test modules
python -m pytest tests/test_config.py -v
python -m pytest tests/test_database.py -v

# Run the test file generator for manual validation
python test.py
```

### Test File Generation

The project includes a comprehensive test file generator:

```bash
python test.py
```

This creates various test files (PDFs, images, documents) in `~/Downloads/_CortexTest/` to validate your sorting rules.

## 🏗️ Architecture

### Core Components

```
Cortex Sorter
├── Configuration Layer (YAML)
├── Database Layer (SQLite)
├── File Analysis Layer (PDF, Hash)
├── Rule Engine (Matching Logic)
├── File Operations (Move, Copy, Monitor)
└── Monitoring System (Watchdog)
```

### Key Classes

- **`Config`**: Configuration management and validation
- **`SortingRule`**: Individual rule definition with matching logic
- **`DatabaseManager`**: SQLite operations with connection pooling
- **`PDFAnalyzer`**: PDF text extraction and keyword matching
- **`FileSorter`**: Main orchestration and rule application
- **`FileOperations`**: Safe file operations with error handling

### Database Schema

The application creates `file_hashes.db` with the following schema:

```sql
CREATE TABLE file_hashes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sha256_hash TEXT UNIQUE NOT NULL,    -- SHA-256 hash of file content
    original_path TEXT NOT NULL,         -- Original file path when first seen
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When file was first processed
    file_size INTEGER,                   -- File size in bytes
    file_name TEXT                       -- Original filename
);
```

**Database indexes:**
- `idx_sha256_hash` - Fast duplicate lookups
- `idx_file_name` - Filename-based queries

**Maintenance:** The database grows over time. To reset duplicate detection, simply delete `file_hashes.db`.

## 📈 Performance & Reliability

### Optimizations
- **Connection Pooling**: Efficient database connections
- **File Stability**: Ensures files are fully written before processing
- **Memory Management**: Streaming hash calculation for large files
- **Concurrent Safety**: WAL mode for database concurrency

### Error Handling
- **Graceful Degradation**: Continues processing despite individual file errors
- **Comprehensive Logging**: Detailed error reporting and debugging
- **Recovery Mechanisms**: Automatic retry logic for transient failures

## 🔧 Development

### Prerequisites
- Python 3.8+
- SQLite 3.0+

### Development Setup

```bash
# Install development dependencies
pip install -r requirements.txt

# Run tests with coverage
pytest --cov=main --cov-report=html

# Generate test files
python test.py

# Lint code
flake8 main.py tests/
```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for your changes
4. Ensure all tests pass (`pytest`)
5. Commit your changes (`git commit -am 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📋 Requirements

### Core Dependencies
- `pdfplumber>=0.9.0` - PDF text extraction
- `watchdog>=3.0.0` - Filesystem monitoring
- `PyYAML>=6.0` - Configuration parsing
- `Pillow>=10.0.0` - Image processing

### Development Dependencies
- `pytest>=7.0.0` - Testing framework
- `pytest-cov>=4.0.0` - Coverage reporting
- `reportlab>=4.0.0` - PDF generation for tests

## 🐛 Troubleshooting

### Common Issues

**"Permission denied" errors:**
- Ensure write access to destination folders
- Check antivirus software isn't blocking file operations

**PDF processing fails:**
- Some PDFs may be encrypted or corrupted
- Check PDF version compatibility (pdfplumber supports most formats)

**High CPU usage:**
- Reduce monitoring frequency in configuration
- Check for filesystem loops (avoid monitoring destination folders)

**Database issues:**
- Delete `file_hashes.db` to reset duplicate detection history
- Check database integrity: `sqlite3 file_hashes.db "PRAGMA integrity_check;"`

### Logs and Debugging

Enable verbose logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Check the database:
```bash
sqlite3 file_hashes.db "SELECT COUNT(*) FROM file_hashes;"
```

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **pdfplumber** for excellent PDF processing capabilities
- **watchdog** for reliable filesystem monitoring
- **SQLite** for robust embedded database functionality

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/cortex-sorter/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/cortex-sorter/discussions)
- **Documentation**: [Wiki](https://github.com/yourusername/cortex-sorter/wiki)

---

**Keep your digital workspace organized with the power of automation! 🤖✨**
