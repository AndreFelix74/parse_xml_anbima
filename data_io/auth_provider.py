#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 17:31:58 2025

@author: andrefelix
"""


"""
Agnostic OAuth2 client credentials authentication module.

Maintains only the state and logic required to obtain and refresh tokens.
Does not contain or depend on any business-specific API logic.
"""


import time
import requests


def new_auth_context(tenant_id, client_id, client_secret, scope, **opts):
    """
    Create a new OAuth2 client credentials authentication context.

    Args:
        tenant_id (str): Azure tenant identifier.
        client_id (str): Client ID of the registered application.
        client_secret (str): Secret associated with the client ID.
        scope (str): Space-separated list of scopes for the token request.
        **opts: Optional parameters, e.g., 'timeout' (int, seconds).

    Returns:
        dict: Authentication context containing credentials, token endpoint,
              and token state.
    """
    return {
        "token_url": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
        "_token": None,
        "_token_exp": 0,
        "timeout": opts.get("timeout", 300),
    }


def _refresh_token(ctx):
    """
    Request a new access token from the OAuth2 server and update the context.

    Args:
        ctx (dict): Authentication context created by new_auth_context.

    Raises:
        requests.HTTPError: If the token request fails.

    Side Effects:
        Updates '_token' and '_token_exp' in the context.
    """

    data = {
        "grant_type": "client_credentials",
        "client_id": ctx["client_id"],
        "client_secret": ctx["client_secret"],
        "scope": ctx["scope"],
    }
    resp = requests.post(ctx["token_url"], data=data, timeout=ctx["timeout"])
    resp.raise_for_status()
    payload = resp.json()
    ctx["_token"] = payload["access_token"]
    ctx["_token_exp"] = time.time() + payload.get("expires_in", 3600) - 60  # margem de segurança


def get_auth_header(ctx):
    """
    Retrieve a valid Authorization header.

    If the current token is missing or expired, a new one is requested.

    Args:
        ctx (dict): Authentication context containing token state.

    Returns:
        dict: Authorization header with the Bearer token.
    """
    if not ctx["_token"] or time.time() >= ctx["_token_exp"]:
        _refresh_token(ctx)
    return {"Authorization": f"Bearer {ctx['_token']}"}
