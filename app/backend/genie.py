"""Genie Conversation API against the 'Personalization Insights' space."""
from __future__ import annotations

import asyncio
from datetime import timedelta

from . import config

_space_id: str | None = None


def space_id() -> str | None:
    global _space_id
    if _space_id:
        return _space_id
    w = config.ws()
    token = None
    while True:
        r = w.genie.list_spaces(page_token=token)
        for s in r.spaces or []:
            if s.title == config.GENIE_SPACE_TITLE:
                _space_id = s.space_id
                return _space_id
        token = r.next_page_token
        if not token:
            return None


def _ask(question: str, conversation_id: str | None) -> dict:
    w = config.ws()
    sid = space_id()
    if not sid:
        raise RuntimeError(f"Genie space '{config.GENIE_SPACE_TITLE}' not found or not shared with the app")
    t = timedelta(seconds=120)
    msg = (w.genie.create_message_and_wait(sid, conversation_id, question, timeout=t) if conversation_id
           else w.genie.start_conversation_and_wait(sid, question, timeout=t))
    out = {"space_id": sid, "conversation_id": msg.conversation_id, "message_id": msg.id,
           "status": msg.status.value if msg.status else None, "text": None, "sql": None, "description": None,
           "columns": [], "rows": [], "suggested_questions": []}
    if msg.error:
        out["text"] = msg.error.error
    for a in msg.attachments or []:
        if a.text and a.text.content:
            out["text"] = a.text.content
        if a.suggested_questions and a.suggested_questions.questions:
            out["suggested_questions"] = list(a.suggested_questions.questions)
        if a.query:
            out["sql"] = a.query.query
            out["description"] = a.query.description
            res = w.genie.get_message_attachment_query_result(sid, msg.conversation_id, msg.id, a.attachment_id)
            sr = res.statement_response
            if sr and sr.manifest and sr.manifest.schema:
                out["columns"] = [c.name for c in sr.manifest.schema.columns]
                out["rows"] = (sr.result.data_array or [])[:200] if sr.result else []
    return out


async def ask(question: str, conversation_id: str | None = None) -> dict:
    return await asyncio.to_thread(_ask, question, conversation_id)
