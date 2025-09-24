#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 16:18:03 2025

@author: andrefelix
"""

"""
Agnostic module for consuming the Maestro API.

Relies on an authentication provider (e.g., auth_provider.get_auth_header)
to inject the Authorization header. It does not handle how tokens are obtained.

Typical usage:
    from auth_provider import new_auth_context, get_auth_header
    ctx_auth = new_auth_context(...)
    ctx_api = new_api_context("https://api-base-url", lambda: get_auth_header(ctx_auth))
    data = api_get(ctx_api, "/investments/Plans")
"""


import requests


def new_api_context(api_base, auth_header_provider, **opts):
    """
    Create a new API context.

    Args:
        api_base (str): Base URL of the API.
        auth_header_provider (callable): Function returning a dict with the Authorization header.
        **opts: Optional parameters, e.g., 'timeout' (int, seconds).

    Returns:
        dict: API context with base URL, auth provider, HTTP session, and timeout.
    """
    return {
        "api_base": api_base.rstrip("/"),
        "auth_header_provider": auth_header_provider,
        "_sess": requests.Session(),
        "timeout": opts.get("timeout", 30),
    }


def _request(ctx, method, endpoint, **kwargs):
    """
    Perform a generic HTTP request to the API.

    Args:
        ctx (dict): API context created by new_api_context.
        method (str): HTTP method (GET, POST, PUT, DELETE).
        endpoint (str): Endpoint path relative to the base URL.
        **kwargs: Additional arguments forwarded to requests.Session.request.

    Returns:
        requests.Response: Response object from the API.

    Raises:
        requests.HTTPError: If the response has status >= 400.
    """
    url = ctx["api_base"] + endpoint
    headers = kwargs.pop("headers", {})
    headers.update(ctx["auth_header_provider"]())

    resp = ctx["_sess"].request(method, url, headers=headers, timeout=ctx["timeout"], **kwargs)
    resp.raise_for_status()
    return resp


def api_get(ctx, endpoint, **params):
    """
    Perform a GET request to the API.

    Args:
        ctx (dict): API context.
        endpoint (str): Endpoint path relative to the base URL.
        **params: Query parameters for the request.

    Returns:
        requests.Response: Response object from the API.
    """
    return _request(ctx, "GET", endpoint, params=params)


def api_post(ctx, endpoint, data=None, json=None):
    """
    Perform a POST request to the API.

    Args:
        ctx (dict): API context.
        endpoint (str): Endpoint path relative to the base URL.
        data (dict or bytes, optional): Form data payload.
        json (dict, optional): JSON payload.

    Returns:
        requests.Response: Response object from the API.
    """
    return _request(ctx, "POST", endpoint, data=data, json=json)


def api_put(ctx, endpoint, data=None, json=None):
    """
    Perform a PUT request to the API.

    Args:
        ctx (dict): API context.
        endpoint (str): Endpoint path relative to the base URL.
        data (dict or bytes, optional): Form data payload.
        json (dict, optional): JSON payload.

    Returns:
        requests.Response: Response object from the API.
    """
    return _request(ctx, "PUT", endpoint, data=data, json=json)


def api_delete(ctx, endpoint):
    """
    Perform a DELETE request to the API.

    Args:
        ctx (dict): API context.
        endpoint (str): Endpoint path relative to the base URL.

    Returns:
        requests.Response: Response object from the API.
    """
    return _request(ctx, "DELETE", endpoint)
