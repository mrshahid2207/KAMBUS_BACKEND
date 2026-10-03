from datetime import date
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base
from models import Student, Bus, Stop, Trip, TemporaryStopChange, MissedBusAllotment, AdminBusChange
from main import cancel_temporary_stop_change, get_current_bus_id, get_effective_student_stop, get_student_my_bus

@pytest.mark.parametrize('replacement_kind', ['temporary', 'missed', 'admin'])
@pytest.mark.parametrize('covered', [False, True])
def test_cancel_restores_registered_assignment(replacement_kind, covered):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with sessionmaker(bind=engine)() as db:
        student = Student(id=1, user_id=1, roll_number='CANCEL', bus_id=1, stop_id=1)
        db.add_all([student, Bus(id=1, bus_number='Registered', route_id=1),
                    Bus(id=3, bus_number='Replacement', route_id=1 if covered else 3),
                    Stop(id=1, name='Registered stop', route_id=1, latitude=18, longitude=79, stop_order=1),
                    Stop(id=3, name='Temporary stop', route_id=3, latitude=19, longitude=80, stop_order=1),
                    Trip(id=1, bus_id=1, route_id=1, status='active'),
                    Trip(id=3, bus_id=3, route_id=3, status='active'),
                    TemporaryStopChange(student_id=1, original_stop_id=1, temporary_stop_id=3,
                                        target_bus_id=3, start_date=date.today(), end_date=date.today(), status='active')])
        if replacement_kind == 'missed':
            db.add(MissedBusAllotment(student_id=1, original_bus_id=1, alternative_bus_id=3,
                                     alternative_trip_id=3, stop_id=3, status='active'))
        if replacement_kind == 'admin':
            db.add(AdminBusChange(source_bus_id=1, target_bus_id=3, start_date=date.today(),
                                  end_date=date.today(), status='active', created_by=1))
        db.commit()
        assert get_current_bus_id(student, db) == 3
        cancel_temporary_stop_change(db, {'user_id': 1})
        db.expire_all()
        expected_bus = 3 if covered and replacement_kind != 'temporary' else 1
        assert get_current_bus_id(student, db) == expected_bus
        assert get_effective_student_stop(db, student)[0].id == 1
        payload = get_student_my_bus(db, {'user_id': 1})
        assert payload['bus_id'] == expected_bus
        assert payload['bus_number'] == ('Registered' if expected_bus == 1 else 'Replacement')
        assert payload['alternative_bus'] is (expected_bus == 3)
        assert payload['active_trip'] is True
    engine.dispose()
