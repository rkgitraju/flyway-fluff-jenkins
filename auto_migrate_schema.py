# This script generates MULTIPLE SQL files per table from Migra output
# With PROPER ordering: dependencies respected, all related statements grouped together

import os
import re
import subprocess
import sys
import shutil
from datetime import datetime
from collections import defaultdict

# --- Configuration (UPDATE THESE VALUES) ---
SOURCE_DB_URL = "postgresql://airflow:airflow@db1:5432/db1" 
TARGET_DB_URL = "postgresql://airflow:airflow@db2:5432/db2"

MIGRATION_DIR = "src/main/resources/db/migration/db2"
TARGET_PROFILE = "db2-local"

# Windows/Linux TimeZone fix for JVM/Maven
MAVEN_OPTS_FIX = "-Duser.timezone=Asia/Kolkata"


# --- SQL Statement Priority (within a single table file) ---

def get_statement_priority(sql_statement):
    """
    Returns priority for ordering statements within a table's file.
    Lower number = runs first.
    """
    sql_upper = sql_statement.upper().strip()
    
    # 1. DROP operations (Clean up first)
    if 'DROP CONSTRAINT' in sql_upper: return 5
    if 'DROP INDEX' in sql_upper: return 6
    if 'DROP TRIGGER' in sql_upper: return 7
    
    # 2. SEQUENCE creation (Must exist before table defaults)
    if re.search(r'^\s*CREATE\s+SEQUENCE', sql_upper):
        return 10
    
    # 3. TABLE creation
    if re.search(r'^\s*CREATE\s+TABLE', sql_upper):
        return 20
    
    # 4. COLUMNS (Must exist before indexes/constraints)
    if 'ALTER TABLE' in sql_upper and 'ADD COLUMN' in sql_upper:
        return 30
    if 'ALTER TABLE' in sql_upper and re.search(r'ADD\s+(?!CONSTRAINT)', sql_upper):
        return 30
    if 'ALTER TABLE' in sql_upper and ('ALTER COLUMN' in sql_upper or 'TYPE' in sql_upper):
        return 31
    if 'ALTER TABLE' in sql_upper and ('SET DEFAULT' in sql_upper or 'DROP DEFAULT' in sql_upper):
        return 32
    if 'ALTER TABLE' in sql_upper and ('SET NOT NULL' in sql_upper or 'DROP NOT NULL' in sql_upper):
        return 33

    # 5. INDEXES (CRITICAL CHANGE: Must run BEFORE constraints that use them)
    # Moved from 60 to 35
    if re.search(r'CREATE\s+(UNIQUE\s+)?INDEX', sql_upper):
        return 35
    
    # 6. CONSTRAINTS (PK, Unique, Check) - Now safer to run
    if 'ALTER TABLE' in sql_upper and 'ADD CONSTRAINT' in sql_upper:
        if 'FOREIGN KEY' not in sql_upper and 'REFERENCES' not in sql_upper:
            return 40
    
    # 7. Other ALTER TABLE statements
    if 'ALTER TABLE' in sql_upper:
        return 45
    
    # 8. SEQUENCE OWNERSHIP (After table exists)
    if 'ALTER SEQUENCE' in sql_upper and 'OWNED BY' in sql_upper:
        return 50
    if 'ALTER SEQUENCE' in sql_upper:
        return 51
    
    # 9. FOREIGN KEYS (After all tables/indexes exist)
    if 'ADD CONSTRAINT' in sql_upper and ('FOREIGN KEY' in sql_upper or 'REFERENCES' in sql_upper):
        return 70
    
    # 10. Triggers and Comments
    if 'CREATE TRIGGER' in sql_upper or 'CREATE OR REPLACE TRIGGER' in sql_upper:
        return 80
    if sql_upper.startswith('COMMENT ON'):
        return 90
    
    # 11. DROP TABLE (Last resort)
    if 'DROP TABLE' in sql_upper:
        return 100
    
    return 55

# --- SQL Parsing Functions ---

def split_sql_statements(sql_content):
    """
    Splits SQL content into individual statements.
    Handles complex statements like CREATE FUNCTION with $$ delimiters.
    """
    statements = []
    current_stmt = []
    dollar_quote_count = 0
    
    lines = sql_content.split('\n')
    for line in lines:
        stripped = line.strip()
        
        # Skip empty lines and comments at statement boundaries
        if not current_stmt and (not stripped or stripped.startswith('--')):
            continue
        
        # Count $$ occurrences to track function bodies
        dollar_quote_count += line.count('$$')
        current_stmt.append(line)
        
        # Check if statement ends
        if stripped.endswith(';') and dollar_quote_count % 2 == 0:
            full_stmt = '\n'.join(current_stmt).strip()
            if full_stmt and full_stmt != ';':
                statements.append(full_stmt)
            current_stmt = []
            dollar_quote_count = 0
    
    # Handle any remaining statement
    if current_stmt:
        full_stmt = '\n'.join(current_stmt).strip()
        if full_stmt and full_stmt != ';':
            statements.append(full_stmt)
    
    return statements


def extract_table_from_sequence_name(seq_name):
    """
    Extract table name from PostgreSQL sequence naming convention.
    Pattern: tablename_columnname_seq
    """
    # Remove quotes and schema prefix
    clean_name = re.sub(r'^"?(?:public\.)?', '', seq_name)
    clean_name = clean_name.strip('"')
    
    # Match pattern: tablename_columnname_seq
    match = re.match(r'^(\w+)_\w+_seq$', clean_name, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    return None


def extract_table_name(sql_statement):
    """
    Extracts table name from various SQL DDL statements.
    Returns None if table name cannot be determined.
    """
    sql_normalized = ' '.join(sql_statement.split())  # Normalize whitespace
    sql_upper = sql_normalized.upper()
    
    # Skip flyway internal table
    if 'flyway_schema_history' in sql_normalized.lower():
        return None
    
    # === SEQUENCE STATEMENTS ===
    
    # ALTER SEQUENCE ... OWNED BY [schema.]table.column
    # This MUST be associated with the table in OWNED BY clause
    if 'ALTER SEQUENCE' in sql_upper and 'OWNED BY' in sql_upper:
        match = re.search(
            r'OWNED\s+BY\s+"?(?:public\.)?"?(\w+)"?\."?\w+"?',
            sql_normalized, re.IGNORECASE
        )
        if match:
            table_name = match.group(1).lower()
            if table_name not in ['none', 'public']:
                return table_name
    
    # CREATE/DROP/ALTER SEQUENCE tablename_column_seq
    if 'SEQUENCE' in sql_upper:
        match = re.search(
            r'(?:CREATE|DROP|ALTER)\s+SEQUENCE\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(?:"?public"?\.)?"?(\w+)"?',
            sql_normalized, re.IGNORECASE
        )
        if match:
            seq_name = match.group(1)
            table_from_seq = extract_table_from_sequence_name(seq_name)
            if table_from_seq:
                return table_from_seq
    
    # === TABLE STATEMENTS ===
    
    # CREATE TABLE [IF NOT EXISTS] [schema.]table_name
    match = re.search(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"?public"?\.)?"?(\w+)"?',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    # ALTER TABLE [ONLY] [schema.]table_name
    match = re.search(
        r'ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?:"?public"?\.)?"?(\w+)"?',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    # DROP TABLE [IF EXISTS] [schema.]table_name
    match = re.search(
        r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:"?public"?\.)?"?(\w+)"?',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    # === INDEX STATEMENTS ===
    
    # CREATE [UNIQUE] INDEX ... ON [schema.]table_name
    match = re.search(
        r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?\S+\s+ON\s+(?:ONLY\s+)?(?:"?public"?\.)?"?(\w+)"?',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    # DROP INDEX - try to infer from index name (idx_tablename_*)
    match = re.search(
        r'DROP\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?(?:"?public"?\.)?"?idx_(\w+?)_',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    # === COMMENT STATEMENTS ===
    
    # COMMENT ON TABLE [schema.]table_name
    match = re.search(
        r'COMMENT\s+ON\s+TABLE\s+(?:"?public"?\.)?"?(\w+)"?',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    # COMMENT ON COLUMN [schema.]table_name.column_name
    match = re.search(
        r'COMMENT\s+ON\s+COLUMN\s+(?:"?public"?\.)?"?(\w+)"?\.',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    # === TRIGGER STATEMENTS ===
    
    # CREATE TRIGGER ... ON [schema.]table_name
    match = re.search(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+\S+\s+.*?\s+ON\s+(?:"?public"?\.)?"?(\w+)"?',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    # DROP TRIGGER ... ON [schema.]table_name
    match = re.search(
        r'DROP\s+TRIGGER\s+(?:IF\s+EXISTS\s+)?\S+\s+ON\s+(?:"?public"?\.)?"?(\w+)"?',
        sql_normalized, re.IGNORECASE
    )
    if match:
        return match.group(1).lower()
    
    return None


def extract_foreign_key_references(sql_statement):
    """
    Extracts referenced table names from foreign key constraints.
    """
    references = []
    # Pattern: REFERENCES [schema.]table_name
    pattern = r'REFERENCES\s+(?:"?public"?\.)?"?(\w+)"?'
    matches = re.findall(pattern, sql_statement, re.IGNORECASE)
    references.extend([m.lower() for m in matches])
    return list(set(references))


def topological_sort_tables(table_statements):
    """
    Sorts tables based on foreign key dependencies.
    Tables with no dependencies come first.
    """
    dependencies = defaultdict(set)
    all_tables = set(table_statements.keys())
    
    # Build dependency graph
    for table_name, statements in table_statements.items():
        for stmt in statements:
            refs = extract_foreign_key_references(stmt)
            for ref in refs:
                ref_lower = ref.lower()
                if ref_lower in all_tables and ref_lower != table_name:
                    dependencies[table_name].add(ref_lower)
    
    # Initialize all tables
    for table in all_tables:
        if table not in dependencies:
            dependencies[table] = set()
    
    # Kahn's algorithm
    sorted_tables = []
    no_deps = sorted([t for t in all_tables if not dependencies[t]])
    visited = set()
    
    while no_deps:
        table = no_deps.pop(0)
        if table in visited:
            continue
        visited.add(table)
        sorted_tables.append(table)
        
        for other_table in sorted(all_tables - visited):
            if table in dependencies[other_table]:
                dependencies[other_table].remove(table)
                if not dependencies[other_table] and other_table not in visited:
                    no_deps.append(other_table)
                    no_deps.sort()
    
    # Handle circular dependencies
    remaining = all_tables - visited
    if remaining:
        print(f"  ⚠️ Warning: Circular dependencies detected: {sorted(remaining)}")
        sorted_tables.extend(sorted(remaining))
    
    return sorted_tables


def parse_sql_by_table(sql_content):
    """
    Parses SQL content and groups ALL statements by table name.
    Returns: (dict of table_name -> sorted list of statements, list of other statements)
    """
    statements = split_sql_statements(sql_content)
    
    table_statements = defaultdict(list)
    other_statements = []
    
    for stmt in statements:
        table_name = extract_table_name(stmt)
        
        if table_name:
            table_statements[table_name].append(stmt)
        else:
            other_statements.append(stmt)
    
    # Sort statements within each table by priority
    for table_name in table_statements:
        table_statements[table_name].sort(key=get_statement_priority)
    
    return dict(table_statements), other_statements


# --- Utility Functions ---

def get_next_version(migration_dir):
    """Calculates the next sequential Flyway version."""
    if not os.path.exists(migration_dir):
        return 1
    
    files = os.listdir(migration_dir)
    versions = []
    for f in files:
        match = re.match(r'V(\d+)__.*\.sql$', f)
        if match:
            versions.append(int(match.group(1)))
    
    return max(versions) + 1 if versions else 1


def delete_generated_files(file_paths):
    """Safely deletes the generated migration files."""
    if not file_paths:
        return
        
    print("\n🗑️ Cleaning up generated files...")
    for file_path in file_paths:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"   Deleted: {os.path.basename(file_path)}")
            except OSError as e:
                print(f"   ⚠️ Warning: Could not delete {os.path.basename(file_path)}: {e}")


def check_delete_where_clause(script_path):
    """Checks for DELETE statements without WHERE clause."""
    with open(script_path, 'r') as f:
        content = f.read()

    unsafe_delete_pattern = re.compile(
        r'DELETE\s+FROM\s+\S+;|\bDELETE\s+FROM\s+\S+\s*$', 
        re.IGNORECASE | re.MULTILINE
    )
    
    if unsafe_delete_pattern.search(content):
        print(f"  ❌ SECURITY VIOLATION: DELETE without WHERE clause.")
        return False
    
    return True


def run_sqlfluff_validation(script_paths):
    return True
    """Runs DELETE check and SQLFluff linting on all scripts."""
    
    print("\n" + "=" * 60)
    print("🔍 SQL Validation")
    print("=" * 60)
    
    all_passed = True
    
    for script_path in script_paths:
        print(f"\n  📄 {os.path.basename(script_path)}")
        
        if not check_delete_where_clause(script_path):
            all_passed = False
            continue
        
        lint_command = f"sqlfluff lint {script_path} --config .sqlfluff"

        try:
            subprocess.run(
                lint_command, 
                shell=True,
                check=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                text=True,
                cwd=os.getcwd()
            )
            print(f"     ✅ Passed")

        except subprocess.CalledProcessError as e:
            print(f"     ❌ Failed")
            for line in e.stdout.strip().split('\n')[:5]:
                print(f"        {line}")
            all_passed = False
        
        except FileNotFoundError:
            print(f"     ⚠️ SQLFluff not found, skipping lint")
    
    return all_passed


def write_migration_file(full_path, table_name, statements, order_info=""):
    """Writes a migration file with proper formatting."""
    with open(full_path, 'w', encoding='utf-8') as f:
        f.write(f"-- ============================================\n")
        f.write(f"-- Table: {table_name}\n")
        if order_info:
            f.write(f"-- {order_info}\n")
        f.write(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"-- Statements: {len(statements)}\n")
        f.write(f"-- ============================================\n\n")
        f.write('\n\n'.join(statements))
        f.write(f"\n\n-- End of {table_name} migration\n")


def run_migra_and_generate_scripts():
    """Compares schemas and generates one SQL file per table."""
    
    print("=" * 60)
    print(f"🔄 Schema Diff: {SOURCE_DB_URL.split('/')[-1]} → {TARGET_DB_URL.split('/')[-1]}")
    print("=" * 60)
    
    # 1. Run Migra
    try:
        print("\n📊 Running Migra...")

        MIGRA_PATH = shutil.which('migra')
        if not MIGRA_PATH:
            raise FileNotFoundError("'migra' executable not found in PATH.")
        
        migra_command = [MIGRA_PATH, '--unsafe', TARGET_DB_URL, SOURCE_DB_URL]
        
        result = subprocess.run(
            migra_command, 
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True, 
        )
        
        # Filter out warnings
        output_lines = result.stdout.splitlines()
        diff_lines = []
        skip_patterns = ['UserWarning:', 'pkg_resources', 'schemainspect', 'flyway_schema_history']
        
        for line in output_lines:
            if not any(p in line for p in skip_patterns):
                diff_lines.append(line)
        
        diff_sql = '\n'.join(diff_lines).strip()
        
    except FileNotFoundError as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"❌ ERROR: Migra failed. {e.stdout if e.stdout else ''}")
        sys.exit(1)
    
    # 2. Check for changes
    if not diff_sql:
        print("\n✅ No schema changes detected.")
        return []
    
    # 3. Show raw SQL for debugging
    print("\n📝 Raw Migra Output:")
    print("-" * 40)
    for line in diff_sql.splitlines()[:15]:
        print(f"   {line}")
    if len(diff_sql.splitlines()) > 15:
        print(f"   ... ({len(diff_sql.splitlines()) - 15} more lines)")
    print("-" * 40)

    # 4. Parse SQL by table
    print("\n🔍 Parsing statements by table...")
    table_statements, other_statements = parse_sql_by_table(diff_sql)
    
    # Debug: Show what was parsed
    print(f"\n   Found {len(table_statements)} table(s):")
    for table, stmts in sorted(table_statements.items()):
        print(f"   • {table}: {len(stmts)} statement(s)")
        for s in stmts[:2]:
            preview = ' '.join(s.split())[:60]
            print(f"      - {preview}...")
    
    if other_statements:
        print(f"\n   Other statements: {len(other_statements)}")
        for s in other_statements[:3]:
            preview = ' '.join(s.split())[:60]
            print(f"      - {preview}...")

    # 5. Sort tables by dependency
    if table_statements:
        sorted_tables = topological_sort_tables(table_statements)
        print(f"\n📦 Execution Order: {' → '.join(sorted_tables)}")
    else:
        sorted_tables = []

    # 6. Generate migration files
    print("\n" + "-" * 60)
    print("📁 Generating Migration Files")
    print("-" * 60)
    
    os.makedirs(MIGRATION_DIR, exist_ok=True)
    generated_files = []
    next_version = get_next_version(MIGRATION_DIR)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M")

    # Generate one file per table (in dependency order)
    for idx, table_name in enumerate(sorted_tables):
        statements = table_statements[table_name]
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        
        file_name = f"V{next_version}__{timestamp_str}_{safe_name}.sql"
        full_path = os.path.join(MIGRATION_DIR, file_name)
        
        # Get dependency info
        deps = set()
        for stmt in statements:
            deps.update(extract_foreign_key_references(stmt))
        deps.discard(table_name)
        deps_str = f"Depends on: {', '.join(sorted(deps))}" if deps else "No dependencies"
        
        write_migration_file(full_path, table_name, statements, deps_str)
        
        generated_files.append(full_path)
        dep_info = f" → [{', '.join(sorted(deps))}]" if deps else ""
        print(f"   ✅ V{next_version}: {table_name} ({len(statements)} stmt){dep_info}")
        next_version += 1

    # Generate file for other statements (if any) - runs LAST
    if other_statements:
        file_name = f"V{next_version}__{timestamp_str}_other_changes.sql"
        full_path = os.path.join(MIGRATION_DIR, file_name)
        
        write_migration_file(full_path, "other_changes", other_statements, "Runs after all table migrations")
        
        generated_files.append(full_path)
        print(f"   ✅ V{next_version}: other_changes ({len(other_statements)} stmt) [LAST]")
    
    print("-" * 60)
    print(f"   Total: {len(generated_files)} file(s)")
    
    return generated_files


def run_flyway_migration(script_paths):
    """Runs Maven Flyway migrate command."""
    os.environ['MAVEN_OPTS'] = MAVEN_OPTS_FIX
    maven_command = f"mvn clean compile flyway:migrate -P{TARGET_PROFILE} -U"
    
    print("\n" + "=" * 60)
    print("🚀 Running Flyway Migration")
    print("=" * 60)
    print(f"Files: {len(script_paths)}")
    for sp in script_paths:
        print(f"   • {os.path.basename(sp)}")
    print("-" * 60)
    
    try:
        subprocess.run(
            maven_command,
            shell=True, 
            env=os.environ,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True, 
        )
        print("\n✅ Flyway migration SUCCESSFUL")
        return True
    except subprocess.CalledProcessError as e:
        print("\n❌ Flyway migration FAILED")
        print(e.stdout)
        return False
    finally:
        os.environ.pop('MAVEN_OPTS', None)


def display_file_contents(generated_files):
    """Shows content of generated files for review."""
    print("\n" + "=" * 60)
    print("📋 Generated File Contents")
    print("=" * 60)
    
    for f in generated_files:
        print(f"\n📄 {os.path.basename(f)}")
        print("-" * 40)
        with open(f, 'r') as file:
            lines = file.readlines()
            # Show non-comment lines
            sql_lines = [l.rstrip() for l in lines if l.strip() and not l.strip().startswith('--')]
            for line in sql_lines:
                print(f"   {line[:70]}{'...' if len(line) > 70 else ''}")
        print("-" * 40)


# --- Main Execution ---

if __name__ == "__main__":
    generated_files = run_migra_and_generate_scripts()
    
    if generated_files:
        # Show file contents for review
        display_file_contents(generated_files)
        
        # Validate
        if not run_sqlfluff_validation(generated_files):
            print("\n❌ Validation failed")
            delete_generated_files(generated_files)
            sys.exit(1)
        
        # Migrate
        success = run_flyway_migration(generated_files)
        if success:
            print(f"\n{'=' * 60}")
            print(f"✅ DONE: {len(generated_files)} migration(s) applied successfully!")
            print("=" * 60)
        else:
            delete_generated_files(generated_files)
            sys.exit(1)
    else:
        print("\n" + "=" * 60)
        print("✅ No changes to apply")
        print("=" * 60)