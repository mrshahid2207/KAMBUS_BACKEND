from database import SessionLocal
from models import User
from auth import hash_password


db = SessionLocal()

try:
    user = User(
        name="Test Student",
        phone="9876543210",
        password_hash=hash_password("kambus123"),
        role="student"
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    print("User created successfully!")
    print("User ID:", user.id)

finally:
    db.close()