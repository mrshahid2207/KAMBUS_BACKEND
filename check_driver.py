from database import SessionLocal
from models import Driver, Bus

db = SessionLocal()
drivers = db.query(Driver).all()
for d in drivers:
    print("user_id:", d.user_id, "| driver.id:", d.id)

bus = db.query(Bus).filter(Bus.id == 4).first()
print("Bus 4 driver_id:", bus.driver_id if bus else "NOT FOUND")