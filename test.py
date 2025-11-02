#!/usr/bin/env python3
"""
Cortex Sorter - Comprehensive Test Suite
Generates test files to validate sorting rules and duplicate detection.
"""

import os
import shutil
from pathlib import Path
from datetime import datetime
import hashlib
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from PIL import Image, ImageDraw, ImageFont

# =============================================================================
# CONFIGURATION
# =============================================================================

TEST_DIR = Path.home() / "Downloads" / "_CortexTest"
BACKUP_DIR = TEST_DIR / "_backup"
RESULTS_FILE = TEST_DIR / "test_results.txt"

# =============================================================================
# TEST FILE GENERATORS
# =============================================================================

class TestFileGenerator:
    """Generates various test files for the sorter."""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def create_pdf_with_content(self, filename: str, content: str) -> Path:
        """Creates a PDF with specific text content."""
        filepath = self.base_dir / filename
        
        c = canvas.Canvas(str(filepath), pagesize=letter)
        width, height = letter
        
        # Add title
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, height - 50, filename)
        
        # Add content
        c.setFont("Helvetica", 12)
        text_object = c.beginText(50, height - 100)
        text_object.setFont("Helvetica", 12)
        text_object.setLeading(14)
        
        # Wrap text
        for line in content.split('\n'):
            text_object.textLine(line)
        
        c.drawText(text_object)
        c.save()
        
        print(f"✓ Created PDF: {filename}")
        return filepath
    
    def create_image(self, filename: str, text: str = None) -> Path:
        """Creates a test image file."""
        filepath = self.base_dir / filename
        
        # Create a simple colored image
        img = Image.new('RGB', (800, 600), color=(73, 109, 137))
        
        if text:
            draw = ImageDraw.Draw(img)
            # Use default font
            draw.text((50, 50), text, fill=(255, 255, 255))
        
        img.save(filepath)
        print(f"✓ Created Image: {filename}")
        return filepath
    
    def create_text_file(self, filename: str, content: str) -> Path:
        """Creates a simple text file."""
        filepath = self.base_dir / filename
        filepath.write_text(content)
        print(f"✓ Created Text: {filename}")
        return filepath
    
    def create_empty_file(self, filename: str) -> Path:
        """Creates an empty file with specific extension."""
        filepath = self.base_dir / filename
        filepath.touch()
        print(f"✓ Created Empty: {filename}")
        return filepath

# =============================================================================
# TEST SCENARIOS
# =============================================================================

class TestScenarios:
    """Defines all test scenarios."""
    
    def __init__(self, generator: TestFileGenerator):
        self.gen = generator
        self.test_results = []
    
    def run_all_tests(self):
        """Runs all test scenarios."""
        print("\n" + "=" * 70)
        print("CORTEX SORTER - TEST SUITE")
        print("=" * 70)
        
        self.test_financial_documents()
        self.test_work_documents()
        self.test_screenshots()
        self.test_installers_archives()
        self.test_general_catchall()
        self.test_duplicate_detection()
        self.test_edge_cases()
        
        self.print_summary()
    
    def test_financial_documents(self):
        """Test financial document sorting."""
        print("\n Testing Financial Documents...")
        
        # Tax documents
        self.gen.create_pdf_with_content(
            "2024_tax_return.pdf",
            "Tax Return\nIRS Form 1040\nFiling Year: 2024\n"
            "This is your official tax return document."
        )
        
        # Invoice
        self.gen.create_pdf_with_content(
            "invoice_12345.pdf",
            "INVOICE #12345\n\nBill To: John Doe\n"
            "Amount Due: $599.99\nPayment Terms: Net 30"
        )
        
        # Bank statement
        self.gen.create_pdf_with_content(
            "statement_jan_2024.pdf",
            "Bank Statement\nAccount Statement - January 2024\n"
            "Opening Balance: $5,000.00\nClosing Balance: $4,500.00\n"
            "Transaction History follows..."
        )
        
        # Insurance
        self.gen.create_pdf_with_content(
            "insurance_policy.pdf",
            "Insurance Certificate\nPolicy Number: POL-123456\n"
            "Coverage: $100,000\nPremium: $150/month"
        )
    
    def test_work_documents(self):
        """Test work-related document sorting."""
        print("\n💼 Testing Work Documents...")
        
        # Confidential report
        self.gen.create_pdf_with_content(
            "Q4_2024_report.pdf",
            "Quarterly Report - Q4 2024\n\nCONFIDENTIAL\n"
            "Internal Use Only\n\nExecutive Summary:\n"
            "This quarter showed significant growth..."
        )
        
        # Meeting notes
        self.gen.create_text_file(
            "meeting_notes_2024-11-02.txt",
            "Meeting Notes - Project Kickoff\nDate: Nov 2, 2024\n"
            "Attendees: Team Alpha\nAgenda: Q1 Planning"
        )
        
        # Contract
        self.gen.create_pdf_with_content(
            "service_agreement.pdf",
            "Service Agreement\n\nThis Agreement is entered into by and between...\n"
            "Terms and Conditions:\n1. Both parties hereby agree..."
        )
    
    def test_screenshots(self):
        """Test screenshot detection."""
        print("\n Testing Screenshots...")
        
        self.gen.create_image("Screenshot 2024-11-02 at 10.30.45 AM.png", "Screenshot Content")
        self.gen.create_image("Screen Shot 2024-11-02.png", "Screen Capture")
        self.gen.create_image("screencapture-website-2024.png", "Web Capture")
    
    def test_installers_archives(self):
        """Test installer and archive detection."""
        print("\n Testing Installers & Archives...")
        
        self.gen.create_empty_file("app_installer_v2.3.dmg")
        self.gen.create_empty_file("setup_wizard.exe")
        self.gen.create_empty_file("project_backup.zip")
        self.gen.create_empty_file("data_export.tar.gz")
    
    def test_general_catchall(self):
        """Test general file sorting."""
        print("\n Testing General Files...")
        
        self.gen.create_pdf_with_content(
            "random_document.pdf",
            "This is just a random PDF document\n"
            "with no specific keywords that would trigger\n"
            "any specialized rules."
        )
        
        self.gen.create_image("vacation_photo.jpg", "Summer 2024")
        self.gen.create_text_file("notes.txt", "Random notes about stuff")
        self.gen.create_empty_file("presentation.pptx")
    
    def test_duplicate_detection(self):
        """Test duplicate file detection."""
        print("\n Testing Duplicate Detection...")
        
        # Create original file
        original_content = "This is the original invoice content\nInvoice #001"
        original = self.gen.create_pdf_with_content(
            "original_invoice.pdf",
            original_content
        )
        
        # Wait a moment, then create duplicate
        import time
        time.sleep(0.5)
        
        # Create exact duplicate (same content, different name)
        duplicate = self.gen.create_pdf_with_content(
            "duplicate_invoice_copy.pdf",
            original_content
        )
        
        print("    Created duplicate pair - should detect identical content")
    
    def test_edge_cases(self):
        """Test edge cases and special scenarios."""
        print("\n Testing Edge Cases...")
        
        # File with multiple matching criteria
        self.gen.create_pdf_with_content(
            "confidential_invoice_report.pdf",
            "CONFIDENTIAL\nINVOICE #789\nQuarterly Report\n"
            "This should match the FIRST rule that applies"
        )
        
        # File with special characters
        self.gen.create_image("Screenshot (1) [final] #2.png", "Special chars")
        
        # Very long filename
        long_name = "a" * 100 + "_screenshot.png"
        self.gen.create_image(long_name, "Long filename test")
        
        # Hidden file (should be ignored)
        self.gen.create_text_file(".hidden_file.txt", "Should be ignored")
        
        # Temp file (should be ignored)
        self.gen.create_empty_file("document.tmp")
    
    def print_summary(self):
        """Prints test summary and instructions."""
        print("\n" + "=" * 70)
        print("TEST FILES CREATED SUCCESSFULLY")
        print("=" * 70)
        print(f"\n Location: {self.gen.base_dir}")
        print("\n Next Steps:")
        print("1. Copy these files to ~/Downloads")
        print("2. Start the Cortex Sorter")
        print("3. Observe the sorting behavior")
        print("4. Check the logs for rule matching")
        print("5. Verify files are in correct destinations")
        print("\n Expected Behavior:")
        print("  • Financial PDFs → ~/Documents/Finance/[category]")
        print("  • Work docs → ~/Documents/Work/[category]")
        print("  • Screenshots → ~/Pictures/Screenshots")
        print("  • Installers → ~/Downloads/_Installers/[os]")
        print("  • Duplicates → ~/Downloads/_Duplicates")
        print("  • General files → ~/Documents or ~/Pictures/Misc")
        print("\n The duplicate invoice should be detected and moved to _Duplicates")
        print("=" * 70)

# =============================================================================
# MANUAL TEST HELPER
# =============================================================================

def create_test_checklist():
    """Creates a testing checklist for manual verification."""
    checklist = """
# CORTEX SORTER - MANUAL TEST CHECKLIST
Generated: {date}

## Pre-Test Setup
[ ] Backup your current Downloads folder
[ ] Ensure Cortex Sorter is NOT running
[ ] Review rules.yml configuration
[ ] Clear any existing test files

## Test Execution
[ ] Run test file generator (python test_cortex.py)
[ ] Copy test files to ~/Downloads
[ ] Start Cortex Sorter
[ ] Wait 5-10 seconds for processing

## Verification Checklist

### Financial Documents
[ ] Tax return → ~/Documents/Finance/Taxes
[ ] Invoice → ~/Documents/Finance/Invoices  
[ ] Bank statement → ~/Documents/Finance/Statements
[ ] Insurance policy → ~/Documents/Finance/Insurance

### Work Documents
[ ] Confidential report → ~/Documents/Work/Confidential
[ ] Meeting notes → ~/Documents/Work/Meetings
[ ] Contract → ~/Documents/Legal/Contracts

### Media Files
[ ] Screenshots → ~/Pictures/Screenshots (all 3 files)
[ ] General photo → ~/Pictures/Misc

### Installers & Archives
[ ] .dmg file → ~/Downloads/_Installers/macOS
[ ] .exe file → ~/Downloads/_Installers/Windows
[ ] .zip file → ~/Downloads/_Archives
[ ] .tar.gz file → ~/Downloads/_Archives

### Duplicate Detection
[ ] Original invoice sorted correctly
[ ] Duplicate invoice → ~/Downloads/_Duplicates
[ ] Check database: sqlite3 file_hashes.db "SELECT * FROM file_hashes;"

### Edge Cases
[ ] Multi-criteria PDF matched FIRST applicable rule
[ ] Files with special characters handled correctly
[ ] Long filename handled correctly
[ ] Hidden files (.hidden_file.txt) ignored
[ ] Temp files (.tmp) ignored

### Database Verification
[ ] Run: sqlite3 file_hashes.db "SELECT COUNT(*) FROM file_hashes;"
[ ] Verify hash count matches processed files
[ ] Check for any errors in logs

## Performance Check
[ ] All files processed within 10 seconds
[ ] No crashes or errors in logs
[ ] CPU usage reasonable during processing

## Cleanup
[ ] Review sorted files in destination folders
[ ] Check ~/Downloads for any unsorted files
[ ] Review logs for warnings or errors
[ ] Delete test files if satisfied

## Issues Found
(Note any problems below)

---
Issue #1:
Description:
Expected:
Actual:

---
Issue #2:
Description:
Expected:
Actual:

""".format(date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    checklist_path = TEST_DIR / "TEST_CHECKLIST.md"
    checklist_path.write_text(checklist)
    print(f"✓ Created test checklist: {checklist_path}")

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def copy_to_downloads():
    """Copies test files to actual Downloads folder."""
    downloads = Path.home() / "Downloads"
    test_files = list(TEST_DIR.glob("*"))
    test_files = [f for f in test_files if f.is_file() and not f.name.startswith("_")]
    
    print(f"\n Ready to copy {len(test_files)} files to Downloads")
    response = input("Proceed? (yes/no): ").strip().lower()
    
    if response in ['yes', 'y']:
        for file in test_files:
            if not file.name.endswith('.md'):  # Don't copy checklist
                dest = downloads / file.name
                shutil.copy2(file, dest)
                print(f"  → Copied: {file.name}")
        print("\n✓ All test files copied to Downloads")
        print("  Start Cortex Sorter now and watch the magic happen!")
    else:
        print("  Cancelled. Files remain in test directory.")

def cleanup_test_files():
    """Removes test directory."""
    if TEST_DIR.exists():
        response = input(f"Delete test directory {TEST_DIR}? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            shutil.rmtree(TEST_DIR)
            print("✓ Test directory cleaned up")

def view_database():
    """Shows database contents for verification."""
    db_path = Path("file_hashes.db")
    if not db_path.exists():
        print(" Database not found. Run Cortex Sorter first.")
        return
    
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    print("\n" + "=" * 70)
    print("DATABASE CONTENTS")
    print("=" * 70)
    
    cursor.execute("SELECT COUNT(*) FROM file_hashes")
    count = cursor.fetchone()[0]
    print(f"\nTotal entries: {count}")
    
    cursor.execute("""
        SELECT file_name, sha256_hash, first_seen 
        FROM file_hashes 
        ORDER BY first_seen DESC 
        LIMIT 20
    """)
    
    print("\nRecent entries:")
    print("-" * 70)
    for row in cursor.fetchall():
        print(f"  {row[2][:19]} | {row[0][:30]:30} | {row[1][:12]}...")
    
    conn.close()

# =============================================================================
# MAIN MENU
# =============================================================================

def main_menu():
    """Interactive menu for testing."""
    while True:
        print("\n" + "=" * 70)
        print("CORTEX SORTER - TEST SUITE")
        print("=" * 70)
        print("1. Generate test files")
        print("2. Copy test files to Downloads")
        print("3. Create test checklist")
        print("4. View database contents")
        print("5. Cleanup test directory")
        print("6. Exit")
        print("=" * 70)
        
        choice = input("\nSelect option (1-6): ").strip()
        
        if choice == '1':
            generator = TestFileGenerator(TEST_DIR)
            scenarios = TestScenarios(generator)
            scenarios.run_all_tests()
        elif choice == '2':
            copy_to_downloads()
        elif choice == '3':
            create_test_checklist()
        elif choice == '4':
            view_database()
        elif choice == '5':
            cleanup_test_files()
        elif choice == '6':
            print(" Goodbye!")
            break
        else:
            print(" Invalid option")

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n\n Test suite interrupted")
    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()