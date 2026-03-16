import hashlib
import hmac
import json
import os
import socket
from pathlib import Path
import tomllib
from uuid import uuid4
from urllib import error, request

import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError

def _load_local_secrets() -> dict:
    local_path = Path(__file__).resolve().parent / "secrets.toml"
    if not local_path.exists():
        return {}
    try:
        with local_path.open("rb") as f:
            data = tomllib.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_LOCAL_SECRETS = _load_local_secrets()


def _get_secret(key: str, default: str = "") -> str:
    # Order of precedence: Streamlit secrets -> env var -> root secrets.toml -> default
    try:
        value = st.secrets.get(key)
        if value is not None:
            return str(value)
    except (StreamlitSecretNotFoundError, FileNotFoundError, KeyError):
        pass

    env_value = os.getenv(key)
    if env_value is not None:
        return env_value

    local_value = _LOCAL_SECRETS.get(key)
    if local_value is not None:
        return str(local_value)

    return default


API_QUERY_URL = _get_secret("API_QUERY_URL", "http://127.0.0.1:8000/query")
API_BASE_URL = API_QUERY_URL.rsplit("/", 1)[0]
USERID_APP = _get_secret("USERID_APP", "")
PASSWORD_APP = _get_secret("PASSWORD_APP", "")
API_TIMEOUT_SECONDS = 90


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _is_valid_login(username: str, password: str) -> bool:
    expected_user_hash = _sha256(USERID_APP.strip())
    expected_password_hash = _sha256(PASSWORD_APP.strip())

    user_hash = _sha256(username.strip())
    password_hash = _sha256(password.strip())

    user_ok = hmac.compare_digest(user_hash, expected_user_hash)
    password_ok = hmac.compare_digest(password_hash, expected_password_hash)
    return user_ok and password_ok


def _query_backend(user_query: str, session_id: str) -> dict:
    payload = {
        "query": user_query,
        "include_debug": False,
        "session_id": session_id,
    }

    req = request.Request(
        API_QUERY_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=API_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except error.HTTPError as exc:
        if exc.code in {502, 503, 504}:
            msg = (
                "The backend took too long or is temporarily unavailable. "
                "Please try again in a minute."
            )
        else:
            msg = f"Backend request failed with HTTP {exc.code}."
        return {"success": False, "answer": "", "metadata": {"error_message": msg}}
    except (TimeoutError, socket.timeout):
        return {
            "success": False,
            "answer": "",
            "metadata": {
                "error_message": (
                    f"Request timed out after {API_TIMEOUT_SECONDS}s. "
                    "Please simplify the question and try again."
                )
            },
        }
    except error.URLError as exc:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            timeout_msg = (
                f"Request timed out after {API_TIMEOUT_SECONDS}s. "
                "Please simplify the question and try again."
            )
            return {"success": False, "answer": "", "metadata": {"error_message": timeout_msg}}
        return {
            "success": False,
            "answer": "",
            "metadata": {"error_message": "Could not reach backend service. Please try again."},
        }
    except Exception as exc:
        return {"success": False, "answer": "", "metadata": {"error_message": str(exc)}}


def _get_sessions() -> list[dict]:
    req = request.Request(
        f"{API_BASE_URL}/sessions",
        headers={"Content-Type": "application/json"},
        method="GET",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _get_session_context(session_id: str) -> list[dict]:
    req = request.Request(
        f"{API_BASE_URL}/sessions/{session_id}/context",
        headers={"Content-Type": "application/json"},
        method="GET",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _context_to_conversations(turns: list[dict]) -> list[dict]:
    conversations = []
    pending_question = ""
    for turn in turns:
        role = turn.get("role")
        content = str(turn.get("content", ""))
        if role == "user":
            pending_question = content
        elif role == "assistant" and pending_question:
            conversations.append({"question": pending_question, "answer": content})
            pending_question = ""
    return conversations[-3:]


def _init_state() -> None:
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid4())
    if "current_question" not in st.session_state:
        st.session_state.current_question = ""
    if "current_answer" not in st.session_state:
        st.session_state.current_answer = ""
    if "current_error" not in st.session_state:
        st.session_state.current_error = ""
    if "conversations" not in st.session_state:
        st.session_state.conversations = []


def _show_login() -> None:
    st.title("Snowflake Agent Login")
    st.caption("Enter credentials to access the testing app.")

    if not USERID_APP or not PASSWORD_APP:
        st.error("Missing USERID_APP or PASSWORD_APP in Streamlit secrets.")
        st.stop()

    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("User ID")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        if _is_valid_login(username, password):
            st.session_state.authenticated = True
            st.success("Login successful.")
            st.rerun()
        else:
            st.error("Invalid credentials.")

    st.stop()


def _show_app() -> None:
    st.title("Snowflake US Census Agent")
    st.caption("Conversation context is preserved per Session. You can start a new session at any time.")

    sessions = _get_sessions()
    session_ids = [s.get("session_id", "") for s in sessions if s.get("session_id")]
    session_titles = {
        s.get("session_id", ""): s.get("title", "New Session")
        for s in sessions
        if s.get("session_id")
    }
    if st.session_state.session_id not in session_ids:
        session_ids = [st.session_state.session_id] + session_ids
        session_titles[st.session_state.session_id] = "New Session"

    top_left, top_mid, top_right = st.columns([3, 2, 1])
    with top_left:
        if session_ids:
            selected_session = st.selectbox(
                "Previous sessions",
                options=session_ids,
                index=session_ids.index(st.session_state.session_id),
                format_func=lambda sid: session_titles.get(sid, sid),
            )
            if selected_session != st.session_state.session_id:
                st.session_state.session_id = selected_session
                st.session_state.conversations = _context_to_conversations(
                    _get_session_context(selected_session)
                )
                st.session_state.current_error = ""
                st.rerun()

    with top_mid:
        if st.button("Start New Session"):
            st.session_state.session_id = str(uuid4())
            st.session_state.current_question = ""
            st.session_state.current_answer = ""
            st.session_state.current_error = ""
            st.session_state.conversations = []
            st.rerun()

    with top_right:
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.session_id = str(uuid4())
            st.session_state.current_question = ""
            st.session_state.current_answer = ""
            st.session_state.current_error = ""
            st.session_state.conversations = []
            st.rerun()

    with st.form("query_form", clear_on_submit=True):
        query = st.text_input("Ask a question")
        submitted = st.form_submit_button("Submit")

    if submitted and query.strip():
        with st.spinner("Thinking..."):
            result = _query_backend(query.strip(), st.session_state.session_id)
        st.session_state.current_question = query.strip()
        st.session_state.current_answer = result.get("answer", "")
        st.session_state.current_error = result.get("metadata", {}).get("error_message", "")
        st.session_state.conversations.append(
            {
                "question": st.session_state.current_question,
                "answer": st.session_state.current_answer,
            }
        )
        st.session_state.conversations = st.session_state.conversations[-3:]
        st.session_state.session_id = result.get("session_id") or st.session_state.session_id

    for i, c in enumerate(reversed(st.session_state.conversations)):
        st.markdown(f"**Q:** {c['question']}")
        st.write(c["answer"])
        if i < len(st.session_state.conversations) - 1:
            st.divider()
    if st.session_state.current_error:
        st.error(st.session_state.current_error)


def main() -> None:
    st.set_page_config(page_title="Snowflake Testing UI", page_icon=":snowflake:")
    _init_state()
    if not st.session_state.authenticated:
        _show_login()
    _show_app()


if __name__ == "__main__":
    main()
