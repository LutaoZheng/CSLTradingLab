"""Print a scrypt hash for a password entered without terminal echo."""
from getpass import getpass
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.auth import hash_password

password=getpass("Password: ")
confirmation=getpass("Confirm password: ")
if password!=confirmation: raise SystemExit("Passwords do not match")
if len(password)<14: raise SystemExit("Use at least 14 characters")
print(hash_password(password))
