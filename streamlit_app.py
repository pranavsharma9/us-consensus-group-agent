import hashlib
import hmac
import json
import os
from uuid import uuid4
from urllib import error, request

import streamlit as st
from dotenv import load_dotenv


load_dotenv()

API_QUERY_URL = os.getenv("API_QUERY_URL", "http://127.0.0.1:8000/query")
USERID_APP = os.getenv("USERID_APP", "")
PASSWORD_APP = os.getenv("PASSWORD_APP", "")


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
        with request.urlopen(req, timeout=180) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        return {"success": False, "answer": "", "metadata": {"error_message": f"HTTP {exc.code}: {raw}"}}
    except Exception as exc:
        return {"success": False, "answer": "", "metadata": {"error_message": str(exc)}}


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
        st.error("Missing USERID_APP or PASSWORD_APP in environment.")
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
    st.caption("Session is preserved while this browser session is active.")

    top_left, top_right = st.columns([4, 1])
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
        st.session_state.conversations.append({
            "question": st.session_state.current_question,
            "answer": st.session_state.current_answer,
        })
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
