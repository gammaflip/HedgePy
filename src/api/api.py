from src.api import session


def connect(user: str, password: str) -> session.Pool:
    profile = session.Profile(user=user)
    return session.new(password=password, base_profile=profile)
