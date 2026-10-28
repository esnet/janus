from datetime import timedelta

import jwt
import logging
from flask import Flask, jsonify
from flask_jwt_extended import (
    JWTManager,
    jwt_required,
    create_access_token,
    get_jwt_identity,
)
from pydantic import BaseModel

from janus.api.controller import httpauth, admin_required, api as controller_api

log = logging.getLogger(__name__)

class TokenResponse(BaseModel):
    access_token: str

class CheckTokenResponse(BaseModel):
    logged_in_as: str

@controller_api.post("/token", summary="Get Token", responses={"200": TokenResponse})
@httpauth.login_required
def get_token():
    """
    Get Token
    https --verify no -a admin:admin_password POST :5000/api/janus/controller/token
    """
    api_user = httpauth.current_user()
    access_token = create_access_token(identity=api_user)
    return jsonify(access_token=access_token)


@controller_api.get("/token", summary="Check Token", responses={"200": CheckTokenResponse})
@jwt_required()
def check_token():
    """
    Check Token.
    https --verify no GET :5000/api/janus/controller/token Authorization:"Bearer $JWT"
    """
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user)


class JwtUtils:
    ACCESS_TOKEN_EXPIRES_IN_DAYS = 365
    _SECRET_KEY = None
    _JWT_MANAGER = None

    @staticmethod
    def configure_app(app: Flask):
        import os
        import secrets

        if os.environ.get("JWT_SECRET_KEY"):
            log.info("Using jwt secret key from environment")
            JwtUtils._SECRET_KEY = os.environ.get("JWT_SECRET_KEY")
        else:
            log.warning(
                "Generating JWT_SECRET_KEY ...(You can set it using env variable)"
            )
            JwtUtils._SECRET_KEY = secrets.token_hex(32)

        app.config["JWT_SECRET_KEY"] = JwtUtils._SECRET_KEY
        app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(
            days=JwtUtils.ACCESS_TOKEN_EXPIRES_IN_DAYS
        )
        app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(days=30)
        JwtUtils._JWT_MANAGER = JWTManager(app)

    @staticmethod
    def verify_token(encoded_jwt: str, algorithm="HS256") -> str:
        try:
            decoded_payload = jwt.decode(
                encoded_jwt, JwtUtils._SECRET_KEY, algorithms=[algorithm]
            )
            return decoded_payload
        except jwt.exceptions.InvalidTokenError as e:
            raise e
