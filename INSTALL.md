# Installation Instructions

**IMPORTANT**: Make sure you are in the correct directory before running commands!

## Step 1: Navigate to the Project Directory

```bash
cd TurboTrends
```

## Step 2: Verify Files

```bash
ls -la requirements.txt
# Should show: -rw-r--r-- 1 user user 678 [date] requirements.txt

ls -la *.py
# Should show all the Python files
```

## Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 4: Test Basic Functionality

```bash
python test_basic_modules.py
```

## Step 5: Setup Database

1. Install PostgreSQL
2. Create database: `createdb turbotrends`
3. Configure .env file with your database credentials
4. Run migration: `python migrate_database.py`

## Step 6: Start the Bot

```bash
python main.py
```

---

**If you get "No such file or directory" errors, make sure you're in the TurboTrends directory!**