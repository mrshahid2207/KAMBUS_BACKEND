import os

# Tests only: real deployments must set their own JWT_SECRET.
os.environ.setdefault("JWT_SECRET", "test-only-secret-not-for-production-0123456789")
