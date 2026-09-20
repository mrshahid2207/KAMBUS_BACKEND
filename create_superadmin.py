"""
One-time setup: create the super-admin account on the server.

    python create_superadmin.py

Needs DATABASE_URL to be set (the same one the app uses). Only one super-admin can
be created this way. Sign in with the printed ID and the password you choose,
using the Admin tab of the login screen.
"""
import getpass
import sys

from pwdlib import PasswordHash

from database import Base, SessionLocal, engine
from models import User

MIN_PASSWORD_LENGTH = 12


def create_superadmin(db, name: str, phone: str, password: str) -> User:
    name = name.strip()
    phone = phone.strip()
    if not name or not phone:
        raise ValueError("Name and phone are required.")
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters.")
    if db.query(User).filter(User.role == "super_admin").first():
        raise ValueError("A super-admin already exists. This script only creates the first one.")
    if db.query(User).filter(User.phone == phone).first():
        raise ValueError("That phone number is already registered.")

    user = User(
        name=name,
        phone=phone,
        password_hash=PasswordHash.recommended().hash(password),
        role="super_admin",
        is_verified=1,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def main() -> int:
    Base.metadata.create_all(bind=engine)
    name = input("Super-admin name: ")
    phone = input("Phone number: ")
    password = getpass.getpass(f"Password (at least {MIN_PASSWORD_LENGTH} characters): ")
    if password != getpass.getpass("Repeat password: "):
        print("Passwords do not match.")
        return 1

    db = SessionLocal()
    try:
        user = create_superadmin(db, name, phone, password)
    except ValueError as exc:
        print(f"Not created: {exc}")
        return 1
    finally:
        db.close()

    print(f"Super-admin created. Sign in on the Admin tab with ID {user.id} and your password.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
