"""Existing operator API tests explicitly sign in through the real login route."""

import os

from fastapi.testclient import TestClient

from services.central.routers.auth import DEFAULT_PASSWORD, DEFAULT_USER


class OperatorClient(TestClient):
    def __init__(self, app, **kwargs):
        super().__init__(app, **kwargs)
        response = self.post("/auth/login", json={
            "username": os.getenv("OPERATOR_USERNAME", DEFAULT_USER),
            "password": os.getenv("OPERATOR_PASSWORD", DEFAULT_PASSWORD),
        })
        assert response.status_code == 200
