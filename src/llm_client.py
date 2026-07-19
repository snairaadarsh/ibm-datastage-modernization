"""
Unified LLM Client for IBM DataStage Modernization Pipeline.

Supports multiple providers:
  - openai    → OpenAI Chat Completions API
  - gemini    → Google Generative AI API
  - anthropic → Anthropic Messages API
  - ag_chat   → Antigravity IDE agent (via local bridge server)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from typing import Optional

logger = logging.getLogger(__name__)

# ── Emit events to stdout for Node.js pipeline-bridge to consume ───────────────

def emit(event: str, data: dict) -> None:
    """Write a JSON event line to stdout for the Node.js parent process."""
    payload = json.dumps({"event": event, **data}, default=str)
    print(payload, flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# LLM Client
# ─────────────────────────────────────────────────────────────────────────────

class LLMClient:
    """
    Provider-agnostic LLM client.
    Instantiate once per pipeline run and pass to all agents.
    """

    def __init__(
        self,
        provider: str,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        bridge_url: str = "http://localhost:3001",
        job_id: Optional[str] = None,
    ):
        self.provider = provider.lower()
        self.api_key = api_key or os.environ.get("LLM_API_KEY", "")
        self.model = model
        self.bridge_url = bridge_url
        self.job_id = job_id
        # Set by pipeline_server after human review "Add Comment" decision.
        # Prepended to every system prompt so all subsequent agents receive the user's guidance.
        self.user_instruction: str = ""

        # Set default models per provider
        if not self.model:
            defaults = {
                "openai":    "gpt-4o",
                "gemini":    "gemini-1.5-pro",
                "anthropic": "claude-sonnet-4-5",
                "ag_chat":   "ag_chat",
            }
            self.model = defaults.get(self.provider, "gpt-4o")

        logger.info(f"[LLMClient] provider={self.provider} model={self.model}")

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def complete(self, system_prompt: str, user_prompt: str, agent_name: str = "Agent") -> str:
        """
        Send a prompt to the configured LLM and return the text response.
        If self.user_instruction is set (from human review comment), it is prepended
        to the system prompt so all agents incorporate the user's guidance.
        """
        logger.info(f"[LLMClient/{self.provider}] Requesting completion for {agent_name}...")

        # Inject user instruction from human review into every system prompt
        effective_system = system_prompt
        if self.user_instruction:
            effective_system = (
                f"IMPORTANT USER INSTRUCTION (from human review):\n"
                f"{self.user_instruction}\n\n"
                f"Apply the above instruction to your response where relevant.\n\n"
                f"{system_prompt}"
            )

        try:
            if self.provider == "openai":
                return self._openai_complete(effective_system, user_prompt)
            elif self.provider == "gemini":
                return self._gemini_complete(effective_system, user_prompt)
            elif self.provider == "anthropic":
                return self._anthropic_complete(effective_system, user_prompt)
            elif self.provider == "ag_chat":
                return self._ag_chat_complete(effective_system, user_prompt, agent_name)
            else:
                raise ValueError(f"Unknown LLM provider: {self.provider}")
        except Exception as e:
            logger.error(f"[LLMClient] Completion failed: {e}")
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Provider implementations
    # ─────────────────────────────────────────────────────────────────────────

    def _openai_complete(self, system_prompt: str, user_prompt: str) -> str:
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package not installed. Run: pip install openai")

        client = OpenAI(api_key=self.api_key)
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=8192,
        )
        return response.choices[0].message.content.strip()

    def _gemini_complete(self, system_prompt: str, user_prompt: str) -> str:
        try:
            import google.generativeai as genai
        except ImportError:
            raise ImportError("google-generativeai not installed. Run: pip install google-generativeai")

        genai.configure(api_key=self.api_key)
        model = genai.GenerativeModel(
            model_name=self.model,
            system_instruction=system_prompt,
        )
        response = model.generate_content(user_prompt)
        return response.text.strip()

    def _anthropic_complete(self, system_prompt: str, user_prompt: str) -> str:
        try:
            import anthropic
        except ImportError:
            raise ImportError("anthropic package not installed. Run: pip install anthropic")

        client = anthropic.Anthropic(api_key=self.api_key)
        message = client.messages.create(
            model=self.model,
            max_tokens=8192,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return message.content[0].text.strip()

    def _ag_chat_complete(self, system_prompt: str, user_prompt: str, agent_name: str) -> str:
        """
        Sends a prompt to the local Antigravity IDE agent bridge.

        Flow:
          1. POST /api/ag-chat/prompt  → queues the prompt, returns requestId
          2. UI receives it via SSE and displays it to the user in the app
          3. User (Antigravity IDE agent) responds in the chat
          4. UI POSTs the response to /api/ag-chat/response/:requestId
          5. We poll /api/ag-chat/response/:requestId until it's filled
          6. Return the response text
        """
        try:
            import requests as req
        except ImportError:
            raise ImportError("requests package not installed. Run: pip install requests")

        request_id = str(uuid.uuid4())

        # Emit a log event so the UI log terminal shows what's happening
        emit("log", {
            "level": "info",
            "message": f"[{agent_name}] Sending prompt to Antigravity IDE agent...",
            "jobId": self.job_id,
        })

        # POST prompt to bridge
        try:
            resp = req.post(
                f"{self.bridge_url}/api/ag-chat/prompt",
                json={
                    "requestId": request_id,
                    "jobId":     self.job_id,
                    "agentName": agent_name,
                    "systemPrompt": system_prompt,
                    "userPrompt":   user_prompt,
                },
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Failed to submit AG Chat prompt: {e}")

        # Emit waiting event
        emit("ag_chat_waiting", {
            "requestId": request_id,
            "agentName": agent_name,
            "jobId": self.job_id,
        })

        # Poll for response (max 10 minutes)
        max_wait = 600
        poll_interval = 2
        elapsed = 0

        while elapsed < max_wait:
            try:
                poll_resp = req.get(
                    f"{self.bridge_url}/api/ag-chat/response/{request_id}",
                    timeout=5,
                )
                if poll_resp.status_code == 200:
                    data = poll_resp.json()
                    if data.get("response"):
                        emit("log", {
                            "level": "success",
                            "message": f"[{agent_name}] Received response from Antigravity IDE agent.",
                            "jobId": self.job_id,
                        })
                        return data["response"]
            except Exception:
                pass

            time.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(f"AG Chat response timed out after {max_wait}s for request {request_id}")
